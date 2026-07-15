//! `AWS::APPAUTOSCALING::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

impl ResourceProvisioner {
    pub(crate) fn create_application_autoscaling_scalable_target(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let service_namespace = props
            .get("ServiceNamespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ServiceNamespace is required".to_string())?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let scalable_dimension = props
            .get("ScalableDimension")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ScalableDimension is required".to_string())?
            .to_string();
        let min_capacity = props
            .get("MinCapacity")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .ok_or_else(|| "MinCapacity is required".to_string())?;
        let max_capacity = props
            .get("MaxCapacity")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .ok_or_else(|| "MaxCapacity is required".to_string())?;
        if min_capacity > max_capacity {
            return Err("MinCapacity must be <= MaxCapacity".to_string());
        }
        let role_arn = props
            .get("RoleARN")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let suspended_state = props.get("SuspendedState").map(|v| AppasSuspendedState {
            dynamic_scaling_in_suspended: v
                .get("DynamicScalingInSuspended")
                .and_then(|x| x.as_bool()),
            dynamic_scaling_out_suspended: v
                .get("DynamicScalingOutSuspended")
                .and_then(|x| x.as_bool()),
            scheduled_scaling_suspended: v
                .get("ScheduledScalingSuspended")
                .and_then(|x| x.as_bool()),
        });

        let arn = format!(
            "arn:aws:application-autoscaling:{}:{}:scalable-target/{}",
            self.region,
            self.account_id,
            fakecloud_core::ids::short_id(10)
        );
        let role = role_arn.unwrap_or_else(|| {
            let suffix = match service_namespace.as_str() {
                "ecs" => "ECSService",
                "elasticmapreduce" => "EMRContainerService",
                "ec2" => "EC2SpotFleetRequest",
                "appstream" => "ApplicationAutoScaling_AppStreamFleet",
                "dynamodb" => "DynamoDBTable",
                "rds" => "RDSCluster",
                "sagemaker" => "SageMakerEndpoint",
                "lambda" => "LambdaConcurrency",
                "elasticache" => "ElastiCacheRG",
                "cassandra" => "CassandraTable",
                "kafka" => "KafkaCluster",
                _ => "ApplicationAutoScaling_Default",
            };
            format!(
                "arn:aws:iam::{}:role/aws-service-role/applicationautoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_{}",
                self.account_id, suffix
            )
        });

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        let key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
        );
        let target = AppasScalableTarget {
            arn: arn.clone(),
            service_namespace: service_namespace.clone(),
            resource_id: resource_id.clone(),
            scalable_dimension: scalable_dimension.clone(),
            min_capacity,
            max_capacity,
            role_arn: role,
            creation_time: Utc::now(),
            suspended_state,
            predicted_capacity: None,
        };
        account.scalable_targets.insert(key, target);

        Ok(ProvisionResult::new(resource_id.clone())
            .with("ScalableTargetARN", arn)
            .with("ServiceNamespace", service_namespace)
            .with("ScalableDimension", scalable_dimension))
    }

    pub(crate) fn create_application_autoscaling_scaling_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "PolicyName is required".to_string())?
            .to_string();
        let service_namespace = props
            .get("ServiceNamespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ServiceNamespace is required".to_string())?
            .to_string();
        let resource_id = props
            .get("ResourceId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ResourceId is required".to_string())?
            .to_string();
        let scalable_dimension = props
            .get("ScalableDimension")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "ScalableDimension is required".to_string())?
            .to_string();
        let policy_type = props
            .get("PolicyType")
            .and_then(|v| v.as_str())
            .unwrap_or("StepScaling")
            .to_string();
        let step_cfg = props.get("StepScalingPolicyConfiguration").cloned();
        let tt_cfg = props
            .get("TargetTrackingScalingPolicyConfiguration")
            .cloned();
        let pred_cfg = props.get("PredictiveScalingPolicyConfiguration").cloned();

        let target_key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
        );
        let policy_key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
            policy_name.clone(),
        );

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        if !account.scalable_targets.contains_key(&target_key) {
            return Err(format!(
                "No scalable target registered for ServiceNamespace={} ResourceId={} ScalableDimension={}",
                service_namespace, resource_id, scalable_dimension
            ));
        }
        let arn = format!(
            "arn:aws:autoscaling:{}:{}:scalingPolicy:{}:resource/{}/{}:policyName/{}",
            self.region,
            self.account_id,
            Uuid::new_v4(),
            service_namespace,
            resource_id,
            policy_name
        );
        let policy = AppasScalingPolicy {
            arn: arn.clone(),
            policy_name: policy_name.clone(),
            service_namespace: service_namespace.clone(),
            resource_id: resource_id.clone(),
            scalable_dimension: scalable_dimension.clone(),
            policy_type: policy_type.clone(),
            creation_time: Utc::now(),
            step_scaling_policy_configuration: step_cfg,
            target_tracking_scaling_policy_configuration: tt_cfg,
            predictive_scaling_policy_configuration: pred_cfg,
            alarms: Vec::new(),
            last_applied_at: None,
        };
        account.scaling_policies.insert(policy_key, policy);

        Ok(ProvisionResult::new(arn.clone())
            .with("PolicyName", policy_name)
            .with("ServiceNamespace", service_namespace)
            .with("ResourceId", resource_id)
            .with("ScalableDimension", scalable_dimension))
    }

    pub(crate) fn delete_application_autoscaling_scalable_target(
        &self,
        physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let namespace = attributes
            .get("ServiceNamespace")
            .cloned()
            .ok_or_else(|| "ServiceNamespace missing in attributes".to_string())?;
        let resource_id = physical_id.to_string();
        let dimension = attributes
            .get("ScalableDimension")
            .cloned()
            .ok_or_else(|| "ScalableDimension missing in attributes".to_string())?;
        let key = (namespace, resource_id.clone(), dimension);

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        account.scalable_targets.remove(&key);
        account
            .scaling_policies
            .retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 == key.2));
        account
            .scheduled_actions
            .retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 == key.2));
        Ok(())
    }

    pub(crate) fn delete_application_autoscaling_scaling_policy(
        &self,
        _physical_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let policy_name = attributes
            .get("PolicyName")
            .cloned()
            .ok_or_else(|| "PolicyName missing in attributes".to_string())?;
        let namespace = attributes
            .get("ServiceNamespace")
            .cloned()
            .ok_or_else(|| "ServiceNamespace missing in attributes".to_string())?;
        let resource_id = attributes
            .get("ResourceId")
            .cloned()
            .ok_or_else(|| "ResourceId missing in attributes".to_string())?;
        let dimension = attributes
            .get("ScalableDimension")
            .cloned()
            .ok_or_else(|| "ScalableDimension missing in attributes".to_string())?;
        let key = (namespace, resource_id, dimension, policy_name);

        let mut state = self.app_autoscaling_state.write();
        let account = state.accounts.entry(self.account_id.clone()).or_default();
        account.scaling_policies.remove(&key);
        Ok(())
    }
}
