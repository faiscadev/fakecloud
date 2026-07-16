// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EcsService {
    pub(super) fn register_task_definition(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let family = req_str(&body, "family")?.to_string();
        validate_family_name(&family)?;
        let container_definitions = body
            .get("containerDefinitions")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| client_exception("Missing required field: containerDefinitions"))?;
        if container_definitions.is_empty() {
            return Err(client_exception(
                "Task definition must have at least one container",
            ));
        }
        for cd in &container_definitions {
            if cd
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
            {
                return Err(client_exception(
                    "Container definition is missing required field: name",
                ));
            }
            if cd
                .get("image")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
            {
                return Err(client_exception(
                    "Container definition is missing required field: image",
                ));
            }
            // Validate `dependsOn[]` entry shape: `condition` must be
            // one of the AWS-documented values (START/COMPLETE/SUCCESS/
            // HEALTHY). Real ECS rejects unknown conditions with a
            // ClientException at register time.
            if let Some(deps) = cd.get("dependsOn").and_then(|v| v.as_array()) {
                for dep in deps {
                    let Some(cond) = dep.get("condition").and_then(|v| v.as_str()) else {
                        return Err(client_exception(
                            "Container dependency is missing required field: condition",
                        ));
                    };
                    if crate::runtime::DependsOnCondition::parse(cond).is_none() {
                        return Err(client_exception(format!(
                            "Container dependency condition '{cond}' is invalid. Valid values are: START, COMPLETE, SUCCESS, HEALTHY",
                        )));
                    }
                    if dep
                        .get("containerName")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .is_empty()
                    {
                        return Err(client_exception(
                            "Container dependency is missing required field: containerName",
                        ));
                    }
                }
            }
        }
        // Reject cyclic `dependsOn[]` graphs at register time, the way
        // real ECS does. Without this gate the runtime would deadlock at
        // launch waiting on each side of the cycle to come up first.
        if let Some((from, to)) = crate::runtime::find_depends_on_cycle(&container_definitions) {
            return Err(client_exception(format!(
                "Container dependency between '{from}' and '{to}' is cyclic; dependsOn graph must be acyclic",
            )));
        }
        // PassRole trust check on the task + execution roles. Real AWS
        // rejects RegisterTaskDefinition when the role's trust policy
        // doesn't list `ecs-tasks.amazonaws.com`.
        if let Some(role_arn) = opt_str(&body, "taskRoleArn") {
            self.check_pass_role(&request.account_id, role_arn)?;
        }
        if let Some(role_arn) = opt_str(&body, "executionRoleArn") {
            self.check_pass_role(&request.account_id, role_arn)?;
        }

        let tags = parse_tags(&body);
        let requires_compatibilities: Vec<String> = body
            .get("requiresCompatibilities")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        // Compatibilities reflect what the task definition is compatible with.
        // We always claim EC2 and FARGATE since we execute via Docker either
        // way — callers with stricter requirements set `requiresCompatibilities`.
        let compatibilities = vec!["EC2".to_string(), "FARGATE".to_string()];

        let account = request.account_id.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let revision = state.allocate_revision(&family);
        let arn = state.task_definition_arn(&family, revision);
        let td = TaskDefinition {
            family: family.clone(),
            revision,
            task_definition_arn: arn,
            container_definitions,
            status: "ACTIVE".to_string(),
            task_role_arn: opt_str(&body, "taskRoleArn").map(String::from),
            execution_role_arn: opt_str(&body, "executionRoleArn").map(String::from),
            network_mode: opt_str(&body, "networkMode").map(String::from),
            requires_compatibilities,
            compatibilities,
            cpu: opt_str(&body, "cpu").map(String::from),
            memory: opt_str(&body, "memory").map(String::from),
            pid_mode: opt_str(&body, "pidMode").map(String::from),
            ipc_mode: opt_str(&body, "ipcMode").map(String::from),
            volumes: body
                .get("volumes")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            placement_constraints: body
                .get("placementConstraints")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            proxy_configuration: body.get("proxyConfiguration").cloned(),
            inference_accelerators: body
                .get("inferenceAccelerators")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            ephemeral_storage: body.get("ephemeralStorage").cloned(),
            runtime_platform: body.get("runtimePlatform").cloned(),
            requires_attributes: Vec::new(),
            registered_at: Utc::now(),
            registered_by: request.principal.as_ref().map(|p| p.arn.clone()).or(Some(
                Arn::global("iam", &state.account_id, "root").to_string(),
            )),
            deregistered_at: None,
            tags: tags.clone(),
            enable_fault_injection: body.get("enableFaultInjection").and_then(|v| v.as_bool()),
        };
        let td_json = task_definition_to_json(&td);
        state
            .task_definitions
            .entry(family.clone())
            .or_default()
            .insert(revision, td);

        Ok(AwsResponse::ok_json(json!({
            "taskDefinition": td_json,
            "tags": tags_json(&tags),
        })))
    }

    pub(super) fn describe_task_definition(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let td_ref = req_str(&body, "taskDefinition")?;
        let (_, family, rev) = resolve_task_definition_ref(td_ref)?;
        let include_tags = body
            .get("include")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().any(|v| v.as_str() == Some("TAGS")))
            .unwrap_or(false);

        let account = target_account_for_task_definition(request, td_ref);
        let accounts = self.state.read();
        let state = accounts
            .get(&account)
            .ok_or_else(|| task_definition_not_found(td_ref))?;
        let revisions = state
            .task_definitions
            .get(&family)
            .ok_or_else(|| task_definition_not_found(td_ref))?;
        let td = match rev {
            Some(n) => revisions
                .get(&n)
                .ok_or_else(|| task_definition_not_found(td_ref))?,
            None => latest_active_revision(revisions)
                .ok_or_else(|| task_definition_not_found(td_ref))?,
        };
        let mut out = json!({"taskDefinition": task_definition_to_json(td)});
        if include_tags {
            out.as_object_mut()
                .unwrap()
                .insert("tags".into(), tags_json(&td.tags));
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn deregister_task_definition(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let td_ref = req_str(&body, "taskDefinition")?;
        let (_, family, rev) = resolve_task_definition_ref(td_ref)?;
        let rev =
            rev.ok_or_else(|| client_exception("taskDefinition must reference a revision"))?;

        let account = target_account_for_task_definition(request, td_ref);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let revisions = state
            .task_definitions
            .get_mut(&family)
            .ok_or_else(|| task_definition_not_found(td_ref))?;
        let td = revisions
            .get_mut(&rev)
            .ok_or_else(|| task_definition_not_found(td_ref))?;
        td.status = "INACTIVE".to_string();
        td.deregistered_at = Some(Utc::now());
        let snapshot = td.clone();
        Ok(AwsResponse::ok_json(json!({
            "taskDefinition": task_definition_to_json(&snapshot),
        })))
    }

    pub(super) fn delete_task_definitions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let refs: Vec<String> = body
            .get("taskDefinitions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .ok_or_else(|| client_exception("Missing required field: taskDefinitions"))?;
        if refs.is_empty() {
            return Err(client_exception("taskDefinitions must not be empty"));
        }

        let mut deleted = Vec::new();
        let mut failures = Vec::new();
        let account = request.account_id.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        for input in refs {
            let parsed = match resolve_task_definition_ref(&input) {
                Ok((_, family, Some(rev))) => Some((family, rev)),
                Ok(_) => None,
                Err(_) => None,
            };
            let Some((family, rev)) = parsed else {
                failures.push(json!({
                    "arn": input,
                    "reason": "INVALID_REFERENCE",
                    "detail": "Expected family:revision or full task-definition ARN",
                }));
                continue;
            };
            let Some(revisions) = state.task_definitions.get_mut(&family) else {
                failures.push(json!({"arn": input, "reason": "MISSING"}));
                continue;
            };
            let Some(td) = revisions.get_mut(&rev) else {
                failures.push(json!({"arn": input, "reason": "MISSING"}));
                continue;
            };
            if td.status == "ACTIVE" {
                failures.push(json!({
                    "arn": td.task_definition_arn.clone(),
                    "reason": "MUST_BE_INACTIVE",
                    "detail": "Task definitions must be deregistered before they can be deleted",
                }));
                continue;
            }
            td.status = "DELETE_IN_PROGRESS".to_string();
            deleted.push(task_definition_to_json(td));
        }
        Ok(AwsResponse::ok_json(json!({
            "taskDefinitions": deleted,
            "failures": failures,
        })))
    }

    pub(super) fn list_task_definitions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(
            &body,
            "status",
            &["ACTIVE", "INACTIVE", "DELETE_IN_PROGRESS"],
        )?;
        validate_enum_opt(&body, "sort", &["ASC", "DESC"])?;
        let family_prefix = opt_str(&body, "familyPrefix");
        let status = opt_str(&body, "status").unwrap_or("ACTIVE");
        let sort = opt_str(&body, "sort").unwrap_or("ASC");
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .filter(|n| (1..=100).contains(n))
            .map(|n| n as usize)
            .unwrap_or(100);
        let next_token = opt_str(&body, "nextToken").unwrap_or("");

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let mut arns: Vec<String> = Vec::new();
        if let Some(state) = accounts.get(&account) {
            for (family, revisions) in &state.task_definitions {
                if let Some(prefix) = family_prefix {
                    if !family.starts_with(prefix) {
                        continue;
                    }
                }
                for td in revisions.values() {
                    if td.status == status {
                        arns.push(td.task_definition_arn.clone());
                    }
                }
            }
        }
        let (page, next) = paginate_arns_dir(arns, next_token, max_results, sort == "DESC");
        let mut out = json!({"taskDefinitionArns": page});
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("nextToken".into(), json!(n));
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn list_task_definition_families(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(&body, "status", &["ACTIVE", "INACTIVE", "ALL"])?;
        let family_prefix = opt_str(&body, "familyPrefix");
        let status = opt_str(&body, "status").unwrap_or("ACTIVE");
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .filter(|n| (1..=100).contains(n))
            .map(|n| n as usize)
            .unwrap_or(100);
        let next_token = opt_str(&body, "nextToken").unwrap_or("");

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let mut families: Vec<String> = Vec::new();
        if let Some(state) = accounts.get(&account) {
            for (family, revisions) in &state.task_definitions {
                if let Some(prefix) = family_prefix {
                    if !family.starts_with(prefix) {
                        continue;
                    }
                }
                let matches_status = match status {
                    "ACTIVE" => revisions.values().any(|td| td.status == "ACTIVE"),
                    "INACTIVE" => revisions
                        .values()
                        .all(|td| td.status == "INACTIVE" || td.status == "DELETE_IN_PROGRESS"),
                    "ALL" => true,
                    _ => revisions.values().any(|td| td.status == status),
                };
                if matches_status {
                    families.push(family.clone());
                }
            }
        }
        families.sort();
        let (page, next) = paginate_arns(families, next_token, max_results);
        let mut out = json!({"families": page});
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("nextToken".into(), json!(n));
        }
        Ok(AwsResponse::ok_json(out))
    }
}
