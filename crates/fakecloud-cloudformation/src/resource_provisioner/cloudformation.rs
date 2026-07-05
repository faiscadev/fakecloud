//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `cloudformation`.

use super::*;

impl ResourceProvisioner {
    // --- CloudFormation Nested Stack ---

    pub(super) fn create_cloudformation_stack(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;

        let template_body = if let Some(body) = props.get("TemplateBody").and_then(|v| v.as_str()) {
            body.to_string()
        } else if let Some(url) = props.get("TemplateURL").and_then(|v| v.as_str()) {
            self.fetch_template_from_url(url)?
        } else {
            return Err(
                "AWS::CloudFormation::Stack requires TemplateURL or TemplateBody".to_string(),
            );
        };

        let child_parameters =
            if let Some(params) = props.get("Parameters").and_then(|v| v.as_object()) {
                let mut map = BTreeMap::new();
                for (k, v) in params {
                    if let Some(s) = v.as_str() {
                        map.insert(k.clone(), s.to_string());
                    } else {
                        map.insert(k.clone(), v.to_string());
                    }
                }
                map
            } else {
                BTreeMap::new()
            };

        let child_tags = if let Some(tags) = props.get("Tags").and_then(|v| v.as_object()) {
            let mut map = BTreeMap::new();
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            map
        } else {
            BTreeMap::new()
        };

        let parsed = crate::template::parse_template(&template_body, &child_parameters)
            .map_err(|e| format!("Failed to parse nested template: {e}"))?;

        let child_stack_name = format!(
            "{}-Nested-{}",
            resource.logical_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let child_stack_id = format!(
            "arn:aws:cloudformation:{}:{}:stack/{}/{}",
            self.region,
            self.account_id,
            child_stack_name,
            Uuid::new_v4()
        );

        let child_provisioner = ResourceProvisioner {
            sqs_state: self.sqs_state.clone(),
            sns_state: self.sns_state.clone(),
            ssm_state: self.ssm_state.clone(),
            iam_state: self.iam_state.clone(),
            s3_state: self.s3_state.clone(),
            eventbridge_state: self.eventbridge_state.clone(),
            dynamodb_state: self.dynamodb_state.clone(),
            logs_state: self.logs_state.clone(),
            lambda_state: self.lambda_state.clone(),
            secretsmanager_state: self.secretsmanager_state.clone(),
            kinesis_state: self.kinesis_state.clone(),
            kms_state: self.kms_state.clone(),
            ecr_state: self.ecr_state.clone(),
            cloudwatch_state: self.cloudwatch_state.clone(),
            elbv2_state: self.elbv2_state.clone(),
            organizations_state: self.organizations_state.clone(),
            cognito_state: self.cognito_state.clone(),
            rds_state: self.rds_state.clone(),
            ec2_state: self.ec2_state.clone(),
            autoscaling_state: self.autoscaling_state.clone(),
            batch_state: self.batch_state.clone(),
            pipes_state: self.pipes_state.clone(),
            ecs_state: self.ecs_state.clone(),
            acm_state: self.acm_state.clone(),
            elasticache_state: self.elasticache_state.clone(),
            route53_state: self.route53_state.clone(),
            cloudfront_state: self.cloudfront_state.clone(),
            stepfunctions_state: self.stepfunctions_state.clone(),
            wafv2_state: self.wafv2_state.clone(),
            apigateway_state: self.apigateway_state.clone(),
            apigatewayv2_state: self.apigatewayv2_state.clone(),
            ses_state: self.ses_state.clone(),
            app_autoscaling_state: self.app_autoscaling_state.clone(),
            athena_state: self.athena_state.clone(),
            firehose_state: self.firehose_state.clone(),
            glue_state: self.glue_state.clone(),
            eks_state: self.eks_state.clone(),
            servicediscovery_state: self.servicediscovery_state.clone(),
            codeartifact_state: self.codeartifact_state.clone(),
            codecommit_state: self.codecommit_state.clone(),
            efs_state: self.efs_state.clone(),
            elasticbeanstalk_state: self.elasticbeanstalk_state.clone(),
            cloudformation_state: self.cloudformation_state.clone(),
            delivery: self.delivery.clone(),
            lambda_runtime: self.lambda_runtime.clone(),
            rds_runtime: self.rds_runtime.clone(),
            ec2_runtime: self.ec2_runtime.clone(),
            ecs_runtime: self.ecs_runtime.clone(),
            elasticache_runtime: self.elasticache_runtime.clone(),
            // Share the parent's queues so nested-stack container resources are
            // drained and backed/reaped by the same parent op, and any deferred
            // custom-resource invokes are drained alongside the parent's.
            pending_container_spawns: self.pending_container_spawns.clone(),
            pending_container_teardowns: self.pending_container_teardowns.clone(),
            pending_custom_invokes: self.pending_custom_invokes.clone(),
            defer_custom_invokes: self.defer_custom_invokes,
            s3_store: self.s3_store.clone(),
            account_id: self.account_id.clone(),
            region: self.region.clone(),
            stack_id: child_stack_id.clone(),
            strict_unknown_types: self.strict_unknown_types,
        };

        let child_resources = crate::service::provision_stack_resources(
            &child_provisioner,
            &parsed.resources,
            &template_body,
            &child_parameters,
            &std::collections::BTreeMap::new(),
        )
        .map_err(|e| format!("Failed to provision nested stack: {e}"))?;

        let child_outputs = parsed.outputs;

        let stack = crate::state::Stack {
            name: child_stack_name.clone(),
            stack_id: child_stack_id.clone(),
            template: template_body.clone(),
            status: "CREATE_COMPLETE".to_string(),
            resources: child_resources.clone(),
            parameters: child_parameters,
            tags: child_tags,
            created_at: Utc::now(),
            updated_at: None,
            description: parsed.description,
            notification_arns: Vec::new(),
            outputs: child_outputs
                .iter()
                .map(|o| crate::state::StackOutput {
                    key: o.logical_id.clone(),
                    value: o.value.clone(),
                    description: o.description.clone(),
                    export_name: o.export_name.clone(),
                })
                .collect(),
        };

        {
            let mut accounts = self.cloudformation_state.write();
            let state = accounts.get_or_create(&self.account_id);
            state.stacks.insert(child_stack_name.clone(), stack);

            crate::service::record_stack_status_event(
                state,
                &child_stack_id,
                &child_stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_IN_PROGRESS",
            );
            let changes: Vec<crate::service::ResourceChange> = child_resources
                .iter()
                .map(|r| crate::service::ResourceChange {
                    action: crate::service::ResourceChangeAction::Create,
                    logical_id: r.logical_id.clone(),
                    physical_id: r.physical_id.clone(),
                    resource_type: r.resource_type.clone(),
                })
                .collect();
            crate::service::record_stack_events(
                state,
                &child_stack_id,
                &child_stack_name,
                &changes,
            );
            crate::service::record_stack_status_event(
                state,
                &child_stack_id,
                &child_stack_name,
                "AWS::CloudFormation::Stack",
                "CREATE_COMPLETE",
            );
        }

        let mut result = ProvisionResult::new(child_stack_id.clone());
        for output in &child_outputs {
            result.attributes.insert(
                format!("Outputs.{}", output.logical_id),
                output.value.clone(),
            );
        }
        Ok(result)
    }

    pub(super) fn delete_cloudformation_stack(&self, physical_id: &str) -> Result<(), String> {
        let stack = {
            let accounts = self.cloudformation_state.read();
            let state = accounts.get(&self.account_id);
            state.and_then(|s| {
                s.stacks
                    .values()
                    .find(|st| st.stack_id == physical_id)
                    .cloned()
            })
        };

        if let Some(stack) = stack {
            let stack_name = stack.name.clone();
            let stack_id = stack.stack_id.clone();

            for resource in stack.resources.iter().rev() {
                let _ = self.delete_resource(resource);
            }

            {
                let mut accounts = self.cloudformation_state.write();
                let state = accounts.get_or_create(&self.account_id);
                state.stacks.remove(&stack_name);

                crate::service::record_stack_status_event(
                    state,
                    &stack_id,
                    &stack_name,
                    "AWS::CloudFormation::Stack",
                    "DELETE_IN_PROGRESS",
                );
                crate::service::record_stack_status_event(
                    state,
                    &stack_id,
                    &stack_name,
                    "AWS::CloudFormation::Stack",
                    "DELETE_COMPLETE",
                );
            }
        }

        Ok(())
    }

    pub(super) fn get_att_cloudformation_stack(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let accounts = self.cloudformation_state.read();
        let state = accounts.get(&self.account_id)?;
        let stack = state.stacks.values().find(|s| s.stack_id == physical_id)?;

        if let Some(output_key) = attribute.strip_prefix("Outputs.") {
            return stack
                .outputs
                .iter()
                .find(|o| o.key == output_key)
                .map(|o| o.value.clone());
        }

        match attribute {
            "Outputs" => Some(
                serde_json::to_string(
                    &stack
                        .outputs
                        .iter()
                        .map(|o| (o.key.clone(), o.value.clone()))
                        .collect::<std::collections::BTreeMap<String, String>>(),
                )
                .unwrap_or_default(),
            ),
            _ => None,
        }
    }
}
