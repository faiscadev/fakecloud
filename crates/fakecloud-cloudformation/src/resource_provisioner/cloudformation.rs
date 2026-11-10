//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `cloudformation`.

use super::*;
use crate::state::RegionalAccounts;

/// A nested stack's template, parameters and tags, from its resource's
/// properties.
pub(crate) struct NestedStackInputs {
    pub(crate) template_body: String,
    pub(crate) parameters: BTreeMap<String, String>,
    pub(crate) tags: BTreeMap<String, String>,
}

impl ResourceProvisioner {
    // --- CloudFormation Nested Stack ---

    /// The provisioner a nested stack's resources go through: the parent's state
    /// and queues, so what the nested stack spawns, tears down or invokes drains
    /// with the parent's operation, under the child's stack id.
    pub(crate) fn nested_provisioner(&self, child_stack_id: &str) -> ResourceProvisioner {
        ResourceProvisioner {
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
            acmpca_state: self.acmpca_state.clone(),
            config_state: self.config_state.clone(),
            route53resolver_state: self.route53resolver_state.clone(),
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
            firehose_engine: self.firehose_engine.clone(),
            glue_state: self.glue_state.clone(),
            eks_state: self.eks_state.clone(),
            servicediscovery_state: self.servicediscovery_state.clone(),
            codeartifact_state: self.codeartifact_state.clone(),
            codecommit_state: self.codecommit_state.clone(),
            efs_state: self.efs_state.clone(),
            elasticbeanstalk_state: self.elasticbeanstalk_state.clone(),
            mq_state: self.mq_state.clone(),
            kafka_state: self.kafka_state.clone(),
            ka2_state: self.ka2_state.clone(),
            redshift_state: self.redshift_state.clone(),
            docdb_state: self.docdb_state.clone(),
            neptune_state: self.neptune_state.clone(),
            codepipeline_state: self.codepipeline_state.clone(),
            codebuild_state: self.codebuild_state.clone(),
            codedeploy_state: self.codedeploy_state.clone(),
            opensearch_state: self.opensearch_state.clone(),
            emr_state: self.emr_state.clone(),
            sagemaker_state: self.sagemaker_state.clone(),
            backup_state: self.backup_state.clone(),
            appsync_state: self.appsync_state.clone(),
            timestream_state: self.timestream_state.clone(),
            mwaa_state: self.mwaa_state.clone(),
            amplify_state: self.amplify_state.clone(),
            iot_state: self.iot_state.clone(),
            appconfig_state: self.appconfig_state.clone(),
            scheduler_state: self.scheduler_state.clone(),
            cloudformation_state: self.cloudformation_state.clone(),
            delivery: self.delivery.clone(),
            lambda_runtime: self.lambda_runtime.clone(),
            iam_mode: self.iam_mode,
            rds_runtime: self.rds_runtime.clone(),
            ec2_runtime: self.ec2_runtime.clone(),
            ecs_runtime: self.ecs_runtime.clone(),
            elasticache_runtime: self.elasticache_runtime.clone(),
            mq_runtime: self.mq_runtime.clone(),
            kafka_runtime: self.kafka_runtime.clone(),
            opensearch_runtime: self.opensearch_runtime.clone(),
            // Share the parent's queues so nested-stack container resources are
            // drained and backed/reaped by the same parent op, and any deferred
            // custom-resource invokes are drained alongside the parent's.
            pending_container_spawns: self.pending_container_spawns.clone(),
            pending_container_teardowns: self.pending_container_teardowns.clone(),
            pending_custom_invokes: self.pending_custom_invokes.clone(),
            defer_custom_invokes: self.defer_custom_invokes,
            s3_store: self.s3_store.clone(),
            kms_hook: self.kms_hook.clone(),
            quota_provider: self.quota_provider.clone(),
            event_source_lookup: self.event_source_lookup.clone(),
            account_id: self.account_id.clone(),
            region: self.region.clone(),
            stack_id: child_stack_id.to_string(),
            strict_unknown_types: self.strict_unknown_types,
            reused_names: Default::default(),
            reprovision_losses: Default::default(),
            custom_responses: self.custom_responses.clone(),
            custom_failures: self.custom_failures.clone(),
            // A nested stack is guarded by its root, which exists before the
            // child stack is recorded.
            guard_stack_id: self.guard_stack_id.clone(),
        }
    }

    /// What a nested stack resource's properties ask its stack to be.
    pub(crate) fn nested_stack_inputs(
        &self,
        resource: &ResourceDefinition,
        child_stack_name: &str,
    ) -> Result<NestedStackInputs, String> {
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

        let mut parameters =
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
        // The child's pseudo parameters are its own.
        for (key, value) in [
            ("AWS::Region", self.region.clone()),
            ("AWS::AccountId", self.account_id.clone()),
            ("AWS::StackId", self.stack_id.clone()),
            ("AWS::StackName", child_stack_name.to_string()),
            (
                "AWS::Partition",
                crate::template::partition_for_region(&self.region).to_string(),
            ),
            (
                "AWS::URLSuffix",
                crate::template::url_suffix_for_region(&self.region).to_string(),
            ),
        ] {
            parameters.entry(key.to_string()).or_insert(value);
        }
        // Defaults the parent left out, as a stack call would merge them.
        crate::service::CloudFormationService::merge_parameter_defaults(
            &mut parameters,
            &template_body,
        );

        let tags = if let Some(tags) = props.get("Tags").and_then(|v| v.as_object()) {
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
        Ok(NestedStackInputs {
            template_body,
            parameters,
            tags,
        })
    }

    pub(super) fn create_cloudformation_stack(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // AWS names a nested stack `{ParentStack}-{LogicalId}-{SUFFIX}`.
        let child_stack_name = self.physical_name(resource);
        let child_stack_id = self.regional_arn(
            "cloudformation",
            &format!("stack/{}/{}", child_stack_name, Uuid::new_v4()),
        );

        let child_provisioner = self.nested_provisioner(&child_stack_id);
        let NestedStackInputs {
            template_body,
            parameters: child_parameters,
            tags: child_tags,
        } = child_provisioner.nested_stack_inputs(resource, &child_stack_name)?;

        let parsed = crate::template::parse_template(&template_body, &child_parameters)
            .map_err(|e| format!("Failed to parse nested template: {e}"))?;

        // Export names cannot depend on resources, so a taken one fails the child
        // before it provisions anything, as it would a top-level stack.
        if let Some(reason) = crate::service::CloudFormationService::export_collision(
            &self.cloudformation_state,
            &self.account_id,
            &self.region,
            &child_stack_name,
            &child_stack_id,
            &template_body,
            &child_parameters,
        ) {
            return Err(self.failed_child_create(&child_stack_id, &child_stack_name, &reason));
        }

        let child_resources = crate::service::provision_stack_resources(
            &child_provisioner,
            &parsed.resources,
            &template_body,
            &child_parameters,
            &std::collections::BTreeMap::new(),
        )
        .map_err(|failure| {
            for resource in failure.leftovers().iter().rev() {
                if !resource.physical_id.is_empty()
                    && !crate::stack_update::is_plain_retain(resource.deletion_policy.as_deref())
                {
                    let _ = child_provisioner.delete_resource(resource);
                }
            }
            self.failed_child_create(
                &child_stack_id,
                &child_stack_name,
                &format!(
                    "The following resource(s) failed to create: [{}]",
                    failure.logical_id
                ),
            )
        })?;

        // The child's own resources resolve its outputs, not the parse above,
        // which ran before any of them existed.
        let child_outputs = crate::service::CloudFormationService::resolve_template_outputs(
            &template_body,
            &child_parameters,
            &child_resources,
            &self.cloudformation_state,
            &self.account_id,
            &self.region,
        );

        let imported_names = crate::template::imported_names(&template_body, &child_parameters);
        let stack = crate::state::Stack {
            name: child_stack_name.clone(),
            stack_id: child_stack_id.clone(),
            template: template_body.clone(),
            status: "CREATE_COMPLETE".to_string(),
            status_reason: None,
            resources: child_resources.clone(),
            parameters: child_parameters,
            tags: child_tags,
            created_at: Utc::now(),
            updated_at: None,
            description: parsed.description,
            notification_arns: Vec::new(),
            outputs: child_outputs.clone(),
            enable_termination_protection: false,
            pending_update: None,
            cancel_requested: false,
        };

        let conflict = {
            let mut accounts = self.cloudformation_state.write();
            let state = accounts.regional_mut(&self.account_id, &self.region);
            // Under one lock with the registration: a stack that claimed one of
            // these names since the check above is seen here.
            let names: Vec<String> = child_outputs
                .iter()
                .filter_map(|o| o.export_name.clone())
                .collect();
            match crate::stack_update::export_conflict(
                state,
                &child_stack_name,
                &child_stack_id,
                &names,
            ) {
                Some(reason) => Some(reason),
                None => {
                    state.stacks.insert(child_stack_name.clone(), stack);
                    crate::service::CloudFormationService::sync_exports_imports(
                        state,
                        &child_stack_id,
                        &child_stack_name,
                        &child_outputs,
                        &imported_names,
                    );

                    crate::service::record_stack_status_event(
                        state,
                        &child_stack_id,
                        &child_stack_name,
                        "AWS::CloudFormation::Stack",
                        "CREATE_IN_PROGRESS",
                    );
                    crate::service::record_stack_events(
                        state,
                        &child_stack_id,
                        &child_stack_name,
                        &child_resources,
                    );
                    crate::service::record_stack_status_event(
                        state,
                        &child_stack_id,
                        &child_stack_name,
                        "AWS::CloudFormation::Stack",
                        "CREATE_COMPLETE",
                    );
                    None
                }
            }
        };
        if let Some(reason) = conflict {
            for resource in child_resources.iter().rev() {
                let _ = self.delete_resource(resource);
            }
            return Err(self.failed_child_create(&child_stack_id, &child_stack_name, &reason));
        }

        let mut result = ProvisionResult::new(child_stack_id.clone());
        for output in &child_outputs {
            result
                .attributes
                .insert(format!("Outputs.{}", output.key), output.value.clone());
        }
        Ok(result)
    }

    /// A child that failed like a top-level stack does on a taken export name: the
    /// recorded events, under an id no stack keeps, since the parent deletes what
    /// is left of it anyway. Returns the parent's failure reason.
    fn failed_child_create(
        &self,
        child_stack_id: &str,
        child_stack_name: &str,
        reason: &str,
    ) -> String {
        let rollback_reason = format!("{reason}. Rollback requested by user.");
        let mut accounts = self.cloudformation_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        for (status, event_reason) in [
            ("CREATE_IN_PROGRESS", Some("User Initiated")),
            ("ROLLBACK_IN_PROGRESS", Some(rollback_reason.as_str())),
            ("ROLLBACK_COMPLETE", None),
        ] {
            crate::service::record_stack_status_event_with_reason(
                state,
                child_stack_id,
                child_stack_name,
                "AWS::CloudFormation::Stack",
                status,
                event_reason,
            );
        }
        format!("Embedded stack {child_stack_id} was not successfully created: {rollback_reason}")
    }

    pub(super) fn delete_cloudformation_stack(&self, physical_id: &str) -> Result<(), String> {
        let stack = {
            let accounts = self.cloudformation_state.read();
            let state = accounts.regional(&self.account_id, &self.region);
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

            // Honor each child resource's DeletionPolicy when a nested stack is
            // torn down, exactly as the top-level DeleteStack path does — a
            // `Retain` resource inside a nested stack must survive too.
            for resource in stack.resources.iter().rev() {
                let _ = self.delete_resource_respecting_policy(resource);
            }
            for orphan in stack
                .pending_update
                .iter()
                .flat_map(|journal| journal.orphans())
            {
                let _ = self.delete_resource(&orphan);
            }

            {
                let mut accounts = self.cloudformation_state.write();
                if let Some(state) = accounts.regional_get_mut(&self.account_id, &self.region) {
                    state.stacks.remove(&stack_name);
                    state
                        .exports
                        .retain(|_, export| export.exporting_stack_id != stack_id);
                    // What it imported is free to change now.
                    for consumers in state.imports.values_mut() {
                        consumers.retain(|consumer| consumer != &stack_name);
                    }
                    state.imports.retain(|_, consumers| !consumers.is_empty());

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
        }

        Ok(())
    }

    pub(super) fn get_att_cloudformation_stack(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let accounts = self.cloudformation_state.read();
        let state = accounts.regional(&self.account_id, &self.region)?;
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
