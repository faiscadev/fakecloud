use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
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
    delivery: Arc<DeliveryBus>,
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
        delivery: Arc<DeliveryBus>,
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
            delivery,
        }
    }

    fn provisioner(&self, stack_id: &str) -> ResourceProvisioner {
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
            delivery: self.delivery.clone(),
            account_id: cf_state.account_id.clone(),
            region: cf_state.region.clone(),
            stack_id: stack_id.to_string(),
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

        // First pass: parse to get resource definitions (without physical ID resolution)
        let parsed = template::parse_template(template_body, &parameters).map_err(|e| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", e)
        })?;

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

        let provisioner = self.provisioner(&stack_id);
        let mut resources = Vec::new();
        let mut physical_ids: HashMap<String, String> = HashMap::new();

        // Create resources incrementally, re-resolving Refs with known physical IDs.
        // Use multi-pass to handle dependency ordering (resources may reference each
        // other via Ref, and JSON object key order is not guaranteed).
        let mut pending: Vec<&template::ResourceDefinition> = parsed.resources.iter().collect();
        let max_passes = pending.len() + 1;
        for _ in 0..max_passes {
            if pending.is_empty() {
                break;
            }
            let mut still_pending = Vec::new();
            let mut made_progress = false;

            for resource_def in pending {
                let resolved_def = template::resolve_resource_properties(
                    resource_def,
                    template_body,
                    &parameters,
                    &physical_ids,
                )
                .map_err(|e| {
                    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", e)
                })?;

                match provisioner.create_resource(&resolved_def) {
                    Ok(stack_resource) => {
                        physical_ids.insert(
                            stack_resource.logical_id.clone(),
                            stack_resource.physical_id.clone(),
                        );
                        resources.push(stack_resource);
                        made_progress = true;
                    }
                    Err(_) => {
                        still_pending.push(resource_def);
                    }
                }
            }

            pending = still_pending;
            if !made_progress && !pending.is_empty() {
                // No progress made — report the first failure
                let resource_def = pending[0];
                let resolved_def = template::resolve_resource_properties(
                    resource_def,
                    template_body,
                    &parameters,
                    &physical_ids,
                )
                .unwrap_or_else(|_| resource_def.clone());
                let err = provisioner.create_resource(&resolved_def).unwrap_err();
                // Rollback
                for r in &resources {
                    let _ = provisioner.delete_resource(r);
                }
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Failed to create resource {}: {err}",
                        resource_def.logical_id
                    ),
                ));
            }
        }

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

        let mut state = self.state.write();

        // Find stack by name or stack ID
        let stack = state.stacks.values_mut().find(|s| {
            (s.name == stack_name || s.stack_id == stack_name) && s.status != "DELETE_COMPLETE"
        });

        if let Some(stack) = stack {
            let stack_id = stack.stack_id.clone();
            let resources: Vec<_> = stack.resources.clone();

            // Build the provisioner while we still have the stack_id
            // Drop the write lock temporarily so the provisioner can read state
            drop(state);
            let provisioner = self.provisioner(&stack_id);

            // Delete resources in reverse order
            for resource in resources.iter().rev() {
                let _ = provisioner.delete_resource(resource);
            }

            // Re-acquire the write lock to update stack status
            let mut state = self.state.write();
            if let Some(stack) = state.stacks.values_mut().find(|s| s.stack_id == stack_id) {
                stack.status = "DELETE_COMPLETE".to_string();
                stack.resources.clear();
            }
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

        // Get stack_id before write lock for the provisioner
        let found_stack_id = {
            let state = self.state.read();
            state
                .stacks
                .values()
                .find(|s| {
                    (s.name == *stack_name || s.stack_id == *stack_name)
                        && s.status != "DELETE_COMPLETE"
                })
                .map(|s| s.stack_id.clone())
                .unwrap_or_default()
        };

        let provisioner = self.provisioner(&found_stack_id);

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

        // Build physical ID map from existing resources
        let mut physical_ids: HashMap<String, String> = stack
            .resources
            .iter()
            .map(|r| (r.logical_id.clone(), r.physical_id.clone()))
            .collect();

        // Create new resources
        let mut update_failed = false;
        let mut update_error_msg = String::new();
        for resource_def in &parsed.resources {
            if !old_logical_ids.contains(&resource_def.logical_id) {
                let resolved_def = match template::resolve_resource_properties(
                    resource_def,
                    template_body,
                    &new_parameters,
                    &physical_ids,
                ) {
                    Ok(d) => d,
                    Err(e) => {
                        update_failed = true;
                        update_error_msg = format!(
                            "Failed to resolve resource {}: {e}",
                            resource_def.logical_id
                        );
                        continue;
                    }
                };
                match provisioner.create_resource(&resolved_def) {
                    Ok(stack_resource) => {
                        physical_ids.insert(
                            stack_resource.logical_id.clone(),
                            stack_resource.physical_id.clone(),
                        );
                        stack.resources.push(stack_resource);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create resource {} during update: {e}",
                            resource_def.logical_id
                        );
                        update_failed = true;
                        update_error_msg =
                            format!("Failed to create resource {}: {e}", resource_def.logical_id);
                    }
                }
            }
        }

        let stack_id = stack.stack_id.clone();
        stack.template = template_body.clone();
        stack.status = if update_failed {
            "UPDATE_FAILED".to_string()
        } else {
            "UPDATE_COMPLETE".to_string()
        };
        stack.parameters = new_parameters;
        if !new_tags.is_empty() {
            stack.tags = new_tags;
        }
        stack.updated_at = Some(Utc::now());
        stack.description = parsed.description;

        if update_failed {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                update_error_msg,
            ));
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::CloudFormationState;
    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_service() -> CloudFormationService {
        let cf_state = Arc::new(RwLock::new(CloudFormationState::new(
            "123456789012",
            "us-east-1",
        )));
        CloudFormationService::new(
            cf_state,
            Arc::new(RwLock::new(fakecloud_sqs::state::SqsState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ))),
            Arc::new(RwLock::new(fakecloud_sns::state::SnsState::new(
                "123456789012",
                "us-east-1",
            ))),
            Arc::new(RwLock::new(fakecloud_ssm::state::SsmState::new(
                "123456789012",
                "us-east-1",
            ))),
            Arc::new(RwLock::new(fakecloud_iam::state::IamState::new(
                "123456789012",
            ))),
            Arc::new(RwLock::new(fakecloud_s3::state::S3State::new(
                "123456789012",
                "us-east-1",
            ))),
            Arc::new(RwLock::new(
                fakecloud_eventbridge::state::EventBridgeState::new("123456789012", "us-east-1"),
            )),
            Arc::new(RwLock::new(fakecloud_dynamodb::state::DynamoDbState::new(
                "123456789012",
                "us-east-1",
            ))),
            Arc::new(RwLock::new(fakecloud_logs::state::LogsState::new(
                "123456789012",
                "us-east-1",
            ))),
            Arc::new(DeliveryBus::new()),
        )
    }

    fn make_request(action: &str, params: HashMap<String, String>) -> AwsRequest {
        AwsRequest {
            service: "cloudformation".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: params,
            body: bytes::Bytes::new(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }

    #[test]
    fn update_stack_sets_failed_status_on_resource_error() {
        let svc = make_service();

        // Create a stack with just a queue
        let mut create_params = HashMap::new();
        create_params.insert("StackName".to_string(), "test-stack".to_string());
        create_params.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"q1"}}}}"#.to_string(),
        );
        let req = make_request("CreateStack", create_params);
        let result = svc.create_stack(&req);
        assert!(result.is_ok());

        // Update stack adding an SNS subscription with a non-existent topic
        let mut update_params = HashMap::new();
        update_params.insert("StackName".to_string(), "test-stack".to_string());
        update_params.insert(
            "TemplateBody".to_string(),
            r#"{"Resources":{"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"q1"}},"BadSub":{"Type":"AWS::SNS::Subscription","Properties":{"TopicArn":"arn:aws:sns:us-east-1:123456789012:nope","Protocol":"sqs","Endpoint":"arn:aws:sqs:us-east-1:123456789012:q1"}}}}"#.to_string(),
        );
        let req = make_request("UpdateStack", update_params);
        let result = svc.update_stack(&req);

        // Should return an error
        assert!(result.is_err());

        // Stack status should be UPDATE_FAILED
        let state = svc.state.read();
        let stack = state.stacks.get("test-stack").unwrap();
        assert_eq!(stack.status, "UPDATE_FAILED");
    }

    #[test]
    fn create_stack_resolves_ref_to_physical_id() {
        let svc = make_service();

        // Template where subscription Refs the topic
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": { "TopicName": "ref-test-topic" }
                },
                "MySub": {
                    "Type": "AWS::SNS::Subscription",
                    "Properties": {
                        "TopicArn": { "Ref": "MyTopic" },
                        "Protocol": "sqs",
                        "Endpoint": "arn:aws:sqs:us-east-1:123456789012:some-queue"
                    }
                }
            }
        }"#;

        let mut params = HashMap::new();
        params.insert("StackName".to_string(), "ref-stack".to_string());
        params.insert("TemplateBody".to_string(), template.to_string());
        let req = make_request("CreateStack", params);
        let result = svc.create_stack(&req);
        assert!(result.is_ok(), "CreateStack failed: {:?}", result.err());

        // Verify both resources were created
        let state = svc.state.read();
        let stack = state.stacks.get("ref-stack").unwrap();
        assert_eq!(stack.resources.len(), 2);
        assert_eq!(stack.status, "CREATE_COMPLETE");

        // The subscription's physical ID should be an ARN (not just "MyTopic")
        let sub = stack
            .resources
            .iter()
            .find(|r| r.logical_id == "MySub")
            .unwrap();
        assert!(
            sub.physical_id.contains("ref-test-topic"),
            "Subscription physical ID should reference the topic ARN, got: {}",
            sub.physical_id
        );
    }
}
