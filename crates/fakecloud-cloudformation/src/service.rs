use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use std::collections::HashMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_dynamodb::state::SharedDynamoDbState;
use fakecloud_eventbridge::state::SharedEventBridgeState;
use fakecloud_iam::state::SharedIamState;
use fakecloud_logs::state::SharedLogsState;
use fakecloud_s3::state::SharedS3State;
use fakecloud_sns::state::SharedSnsState;
use fakecloud_sqs::state::SharedSqsState;
use fakecloud_ssm::state::SharedSsmState;

use crate::resource_provisioner::ResourceProvisioner;
use crate::state::{SharedCloudFormationState, Stack};
use crate::template;
use crate::xml_responses;

pub struct CloudFormationService {
    state: SharedCloudFormationState,
    sqs_state: SharedSqsState,
    sns_state: SharedSnsState,
    ssm_state: SharedSsmState,
    iam_state: SharedIamState,
    s3_state: SharedS3State,
    eventbridge_state: SharedEventBridgeState,
    dynamodb_state: SharedDynamoDbState,
    logs_state: SharedLogsState,
}

impl CloudFormationService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state: SharedCloudFormationState,
        sqs_state: SharedSqsState,
        sns_state: SharedSnsState,
        ssm_state: SharedSsmState,
        iam_state: SharedIamState,
        s3_state: SharedS3State,
        eventbridge_state: SharedEventBridgeState,
        dynamodb_state: SharedDynamoDbState,
        logs_state: SharedLogsState,
    ) -> Self {
        Self {
            state,
            sqs_state,
            sns_state,
            ssm_state,
            iam_state,
            s3_state,
            eventbridge_state,
            dynamodb_state,
            logs_state,
        }
    }

    fn provisioner(&self) -> ResourceProvisioner {
        let cf_state = self.state.read();
        ResourceProvisioner {
            sqs_state: self.sqs_state.clone(),
            sns_state: self.sns_state.clone(),
            ssm_state: self.ssm_state.clone(),
            iam_state: self.iam_state.clone(),
            s3_state: self.s3_state.clone(),
            eventbridge_state: self.eventbridge_state.clone(),
            dynamodb_state: self.dynamodb_state.clone(),
            logs_state: self.logs_state.clone(),
            account_id: cf_state.account_id.clone(),
            region: cf_state.region.clone(),
        }
    }

    fn get_param(req: &AwsRequest, key: &str) -> Option<String> {
        // Check query params first (for Query protocol)
        if let Some(v) = req.query_params.get(key) {
            return Some(v.clone());
        }
        // Then check form-encoded body
        let body_params = fakecloud_core::protocol::parse_query_body(&req.body);
        body_params.get(key).cloned()
    }

    fn get_all_params(req: &AwsRequest) -> HashMap<String, String> {
        let mut params = req.query_params.clone();
        let body_params = fakecloud_core::protocol::parse_query_body(&req.body);
        for (k, v) in body_params {
            params.entry(k).or_insert(v);
        }
        params
    }

    fn extract_tags(params: &HashMap<String, String>) -> HashMap<String, String> {
        let mut tags = HashMap::new();
        for i in 1.. {
            let key_param = format!("Tags.member.{i}.Key");
            let value_param = format!("Tags.member.{i}.Value");
            match (params.get(&key_param), params.get(&value_param)) {
                (Some(k), Some(v)) => {
                    tags.insert(k.clone(), v.clone());
                }
                _ => break,
            }
        }
        tags
    }

    fn extract_parameters(params: &HashMap<String, String>) -> HashMap<String, String> {
        let mut result = HashMap::new();
        for i in 1.. {
            let key_param = format!("Parameters.member.{i}.ParameterKey");
            let value_param = format!("Parameters.member.{i}.ParameterValue");
            match (params.get(&key_param), params.get(&value_param)) {
                (Some(k), Some(v)) => {
                    result.insert(k.clone(), v.clone());
                }
                _ => break,
            }
        }
        result
    }

    fn create_stack(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let params = Self::get_all_params(req);

        let stack_name = params.get("StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let template_body = params.get("TemplateBody").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "TemplateBody is required",
            )
        })?;

        // Check if stack already exists and is not deleted
        {
            let state = self.state.read();
            if let Some(existing) = state.stacks.get(stack_name.as_str()) {
                if existing.status != "DELETE_COMPLETE" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExistsException",
                        format!("Stack [{stack_name}] already exists"),
                    ));
                }
            }
        }

        let tags = Self::extract_tags(&params);
        let parameters = Self::extract_parameters(&params);

        let parsed = template::parse_template(template_body, &parameters).map_err(|e| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", e)
        })?;

        let provisioner = self.provisioner();
        let mut resources = Vec::new();

        for resource_def in &parsed.resources {
            match provisioner.create_resource(resource_def) {
                Ok(stack_resource) => resources.push(stack_resource),
                Err(e) => {
                    // Rollback: delete all resources created so far
                    for r in &resources {
                        let _ = provisioner.delete_resource(r);
                    }
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationError",
                        format!("Failed to create resource {}: {e}", resource_def.logical_id),
                    ));
                }
            }
        }

        let stack_id = {
            let state = self.state.read();
            format!(
                "arn:aws:cloudformation:{}:{}:stack/{}/{}",
                state.region,
                state.account_id,
                stack_name,
                uuid::Uuid::new_v4()
            )
        };

        let stack = Stack {
            name: stack_name.clone(),
            stack_id: stack_id.clone(),
            template: template_body.clone(),
            status: "CREATE_COMPLETE".to_string(),
            resources,
            parameters,
            tags,
            created_at: Utc::now(),
            updated_at: None,
            description: parsed.description,
        };

        {
            let mut state = self.state.write();
            state.stacks.insert(stack_name.clone(), stack);
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::create_stack_response(&stack_id, &req.request_id),
        ))
    }

    fn delete_stack(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let provisioner = self.provisioner();

        let mut state = self.state.write();

        // Find stack by name or stack ID
        let stack = state.stacks.values_mut().find(|s| {
            (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
        });

        if let Some(stack) = stack {
            // Delete resources in reverse order
            let resources: Vec<_> = stack.resources.clone();
            for resource in resources.iter().rev() {
                let _ = provisioner.delete_resource(resource);
            }
            stack.status = "DELETE_COMPLETE".to_string();
            stack.resources.clear();
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::delete_stack_response(&req.request_id),
        ))
    }

    fn describe_stacks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName");

        let state = self.state.read();
        let stacks: Vec<Stack> = if let Some(ref name) = stack_name {
            state
                .stacks
                .values()
                .filter(|s| {
                    (s.name == *name || s.stack_id == *name) && s.status != "DELETE_COMPLETE"
                })
                .cloned()
                .collect()
        } else {
            state
                .stacks
                .values()
                .filter(|s| s.status != "DELETE_COMPLETE")
                .cloned()
                .collect()
        };

        if let Some(ref name) = stack_name {
            if stacks.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Stack with id {name} does not exist"),
                ));
            }
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::describe_stacks_response(&stacks, &req.request_id),
        ))
    }

    fn list_stacks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let stacks: Vec<Stack> = state.stacks.values().cloned().collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::list_stacks_response(&stacks, &req.request_id),
        ))
    }

    fn list_stack_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let state = self.state.read();
        let stack = state
            .stacks
            .values()
            .find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Stack [{stack_name}] does not exist"),
                )
            })?;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::list_stack_resources_response(&stack.resources, &req.request_id),
        ))
    }

    fn describe_stack_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let state = self.state.read();
        let stack = state
            .stacks
            .values()
            .find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Stack [{stack_name}] does not exist"),
                )
            })?;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::describe_stack_resources_response(
                &stack.resources,
                &stack.name,
                &req.request_id,
            ),
        ))
    }

    fn update_stack(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let params = Self::get_all_params(req);

        let stack_name = params.get("StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let template_body = params.get("TemplateBody").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "TemplateBody is required",
            )
        })?;

        let new_parameters = Self::extract_parameters(&params);
        let new_tags = Self::extract_tags(&params);

        let parsed = template::parse_template(template_body, &new_parameters).map_err(|e| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", e)
        })?;

        let provisioner = self.provisioner();

        let mut state = self.state.write();
        let stack = state
            .stacks
            .values_mut()
            .find(|s| {
                (s.name == *stack_name || s.stack_id == *stack_name)
                    && s.status != "DELETE_COMPLETE"
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Stack [{stack_name}] does not exist"),
                )
            })?;

        // Determine which resources to add and remove
        let old_logical_ids: std::collections::HashSet<String> = stack
            .resources
            .iter()
            .map(|r| r.logical_id.clone())
            .collect();
        let new_logical_ids: std::collections::HashSet<String> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.clone())
            .collect();

        // Delete resources that are no longer in the template
        let to_remove: Vec<_> = stack
            .resources
            .iter()
            .filter(|r| !new_logical_ids.contains(&r.logical_id))
            .cloned()
            .collect();
        for resource in &to_remove {
            let _ = provisioner.delete_resource(resource);
        }
        stack
            .resources
            .retain(|r| new_logical_ids.contains(&r.logical_id));

        // Create new resources
        for resource_def in &parsed.resources {
            if !old_logical_ids.contains(&resource_def.logical_id) {
                match provisioner.create_resource(resource_def) {
                    Ok(stack_resource) => stack.resources.push(stack_resource),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create resource {} during update: {e}",
                            resource_def.logical_id
                        );
                    }
                }
            }
        }

        let stack_id = stack.stack_id.clone();
        stack.template = template_body.clone();
        stack.status = "UPDATE_COMPLETE".to_string();
        stack.parameters = new_parameters;
        if !new_tags.is_empty() {
            stack.tags = new_tags;
        }
        stack.updated_at = Some(Utc::now());
        stack.description = parsed.description;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::update_stack_response(&stack_id, &req.request_id),
        ))
    }

    fn get_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let stack_name = Self::get_param(req, "StackName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "StackName is required",
            )
        })?;

        let state = self.state.read();
        let stack = state
            .stacks
            .values()
            .find(|s| {
                (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Stack [{stack_name}] does not exist"),
                )
            })?;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_responses::get_template_response(&stack.template, &req.request_id),
        ))
    }
}

#[async_trait]
impl AwsService for CloudFormationService {
    fn service_name(&self) -> &str {
        "cloudformation"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateStack" => self.create_stack(&req),
            "DeleteStack" => self.delete_stack(&req),
            "DescribeStacks" => self.describe_stacks(&req),
            "ListStacks" => self.list_stacks(&req),
            "ListStackResources" => self.list_stack_resources(&req),
            "DescribeStackResources" => self.describe_stack_resources(&req),
            "UpdateStack" => self.update_stack(&req),
            "GetTemplate" => self.get_template(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "cloudformation",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateStack",
            "DeleteStack",
            "DescribeStacks",
            "ListStacks",
            "ListStackResources",
            "DescribeStackResources",
            "UpdateStack",
            "GetTemplate",
        ]
    }
}
