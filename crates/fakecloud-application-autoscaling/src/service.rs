//! Application Auto Scaling JSON 1.1 service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_aws::arn::{partition_for, Arn};
use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    AccountState, ApplicationAutoScalingAccounts, NotScaledReason, ScalableTarget,
    ScalableTargetAction, ScalingActivity, ScalingPolicy, ScheduledAction,
    SharedApplicationAutoScalingState, SuspendedState,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "RegisterScalableTarget",
    "DescribeScalableTargets",
    "DeregisterScalableTarget",
    "PutScalingPolicy",
    "DescribeScalingPolicies",
    "DeleteScalingPolicy",
    "PutScheduledAction",
    "DescribeScheduledActions",
    "DeleteScheduledAction",
    "DescribeScalingActivities",
    "GetPredictiveScalingForecast",
    "ListTagsForResource",
    "TagResource",
    "UntagResource",
];

pub struct ApplicationAutoScalingService {
    state: SharedApplicationAutoScalingState,
}

impl ApplicationAutoScalingService {
    pub fn new(state: SharedApplicationAutoScalingState) -> Self {
        Self { state }
    }

    pub fn shared_state(&self) -> SharedApplicationAutoScalingState {
        Arc::clone(&self.state)
    }
}

impl Default for ApplicationAutoScalingService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(ApplicationAutoScalingAccounts::new())))
    }
}

#[async_trait]
impl AwsService for ApplicationAutoScalingService {
    fn service_name(&self) -> &str {
        "application-autoscaling"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "RegisterScalableTarget" => self.register_scalable_target(&req),
            "DescribeScalableTargets" => self.describe_scalable_targets(&req),
            "DeregisterScalableTarget" => self.deregister_scalable_target(&req),
            "PutScalingPolicy" => self.put_scaling_policy(&req),
            "DescribeScalingPolicies" => self.describe_scaling_policies(&req),
            "DeleteScalingPolicy" => self.delete_scaling_policy(&req),
            "PutScheduledAction" => self.put_scheduled_action(&req),
            "DescribeScheduledActions" => self.describe_scheduled_actions(&req),
            "DeleteScheduledAction" => self.delete_scheduled_action(&req),
            "DescribeScalingActivities" => self.describe_scaling_activities(&req),
            "GetPredictiveScalingForecast" => self.get_predictive_scaling_forecast(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            other => Err(AwsServiceError::action_not_implemented(
                "application-autoscaling",
                other,
            )),
        }
    }
}

impl ApplicationAutoScalingService {
    fn register_scalable_target(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let service_namespace = require_str(&body, "ServiceNamespace")?;
        validate_service_namespace(&service_namespace)?;
        let resource_id = require_str(&body, "ResourceId")?;
        validate_resource_id_len("resourceId", &resource_id)?;
        let scalable_dimension = require_str(&body, "ScalableDimension")?;
        validate_scalable_dimension(&scalable_dimension)?;
        let min_capacity = body
            .get("MinCapacity")
            .and_then(Value::as_i64)
            .map(|n| n as i32);
        let max_capacity = body
            .get("MaxCapacity")
            .and_then(Value::as_i64)
            .map(|n| n as i32);
        let role_arn = body
            .get("RoleARN")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(role) = role_arn.as_deref() {
            validate_resource_id_len("roleARN", role)?;
        }
        let suspended_state = body.get("SuspendedState").map(parse_suspended_state);

        let key = (
            service_namespace.clone(),
            resource_id.clone(),
            scalable_dimension.clone(),
        );

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let now = Utc::now();
        let (arn, prev_min, prev_max, was_existing) = if let Some(existing) =
            account.scalable_targets.get_mut(&key)
        {
            let prev_min = existing.min_capacity;
            let prev_max = existing.max_capacity;
            let new_min = min_capacity.unwrap_or(existing.min_capacity);
            let new_max = max_capacity.unwrap_or(existing.max_capacity);
            if new_min > new_max {
                return Err(invalid_param("MinCapacity must be <= MaxCapacity"));
            }
            existing.min_capacity = new_min;
            existing.max_capacity = new_max;
            if let Some(role) = role_arn {
                existing.role_arn = role;
            }
            if let Some(sus) = suspended_state {
                existing.suspended_state = Some(sus);
            }
            (existing.arn.clone(), Some(prev_min), Some(prev_max), true)
        } else {
            let min = min_capacity
                .ok_or_else(|| invalid_param("MinCapacity is required for new scalable targets"))?;
            let max = max_capacity
                .ok_or_else(|| invalid_param("MaxCapacity is required for new scalable targets"))?;
            if min > max {
                return Err(invalid_param("MinCapacity must be <= MaxCapacity"));
            }
            let arn = synth_scalable_target_arn(&req.account_id, &req.region);
            let role = role_arn.unwrap_or_else(|| {
                default_service_linked_role(&req.account_id, &service_namespace)
            });
            let target = ScalableTarget {
                arn: arn.clone(),
                service_namespace: service_namespace.clone(),
                resource_id: resource_id.clone(),
                scalable_dimension: scalable_dimension.clone(),
                min_capacity: min,
                max_capacity: max,
                role_arn: role,
                creation_time: now,
                suspended_state,
                predicted_capacity: None,
            };
            account.scalable_targets.insert(key, target);
            (arn, None, None, false)
        };

        // Real Application Auto Scaling appends a ScalingActivity row
        // whenever bounds change so DescribeScalingActivities surfaces a
        // history. Synthesize matching rows here for both the initial
        // register and any subsequent bound update.
        let cur_min = min_capacity.or(prev_min).unwrap_or(0);
        let cur_max = max_capacity.or(prev_max).unwrap_or(0);
        let bounds_changed =
            !was_existing || prev_min != Some(cur_min) || prev_max != Some(cur_max);
        if bounds_changed {
            let description = if was_existing {
                format!(
                    "Updated min capacity to {cur_min} and max capacity to {cur_max} for {resource_id}"
                )
            } else {
                format!(
                    "Setting min capacity to {cur_min} and max capacity to {cur_max} for {resource_id}"
                )
            };
            let activity = ScalingActivity {
                activity_id: synth_activity_id(),
                service_namespace: service_namespace.clone(),
                resource_id: resource_id.clone(),
                scalable_dimension: scalable_dimension.clone(),
                description,
                cause: "User initiated capacity change via RegisterScalableTarget".to_string(),
                start_time: now,
                end_time: Some(now),
                status_code: "Successful".to_string(),
                status_message: Some("Successfully set scaling target.".to_string()),
                details: None,
                not_scaled_reasons: Vec::new(),
            };
            account.scaling_activities.push(activity);
        }
        Ok(AwsResponse::ok_json(json!({
            "ScalableTargetARN": arn,
        })))
    }

    fn describe_scalable_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        validate_service_namespace(&namespace)?;
        let resource_ids: Vec<String> = body
            .get("ResourceIds")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        for rid in &resource_ids {
            validate_resource_id_len("resourceIds", rid)?;
        }
        let dimension = body
            .get("ScalableDimension")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(d) = dimension.as_deref() {
            validate_scalable_dimension(d)?;
        }
        let max_results = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(50);
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());

        let state = self.state.read();
        let mut all: Vec<ScalableTarget> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.scalable_targets
                    .values()
                    .filter(|t| t.service_namespace == namespace)
                    .filter(|t| resource_ids.is_empty() || resource_ids.contains(&t.resource_id))
                    .filter(|t| {
                        dimension
                            .as_deref()
                            .is_none_or(|d| t.scalable_dimension == d)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        all.sort_by(|a, b| a.arn.cmp(&b.arn));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let mut response = json!({
            "ScalableTargets": page.iter().map(scalable_target_json).collect::<Vec<_>>(),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn deregister_scalable_target(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        let resource_id = require_str(&body, "ResourceId")?;
        let dimension = require_str(&body, "ScalableDimension")?;
        let key = (namespace, resource_id, dimension);

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.scalable_targets.remove(&key).is_none() {
            return Err(object_not_found(format!(
                "No scalable target registered for ServiceNamespace={} ResourceId={} ScalableDimension={}",
                key.0, key.1, key.2
            )));
        }
        // Cascade: real AWS keeps the policies/scheduled actions on the
        // target; cleaning up here keeps state coherent for tests.
        account
            .scaling_policies
            .retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 == key.2));
        account
            .scheduled_actions
            .retain(|k, _| !(k.0 == key.0 && k.1 == key.1 && k.2 == key.2));
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_name = require_str(&body, "PolicyName")?;
        let namespace = require_str(&body, "ServiceNamespace")?;
        let resource_id = require_str(&body, "ResourceId")?;
        let dimension = require_str(&body, "ScalableDimension")?;
        let policy_type = body
            .get("PolicyType")
            .and_then(Value::as_str)
            .unwrap_or("StepScaling")
            .to_string();
        let step_cfg = body.get("StepScalingPolicyConfiguration").cloned();
        let tt_cfg = body
            .get("TargetTrackingScalingPolicyConfiguration")
            .cloned();
        let pred_cfg = body.get("PredictiveScalingPolicyConfiguration").cloned();

        let target_key = (namespace.clone(), resource_id.clone(), dimension.clone());
        let policy_key = (
            namespace.clone(),
            resource_id.clone(),
            dimension.clone(),
            policy_name.clone(),
        );

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.scalable_targets.contains_key(&target_key) {
            return Err(object_not_found(format!(
                "No scalable target registered for ServiceNamespace={namespace} ResourceId={resource_id} ScalableDimension={dimension}"
            )));
        }
        let arn = if let Some(existing) = account.scaling_policies.get_mut(&policy_key) {
            existing.policy_type = policy_type.clone();
            existing.step_scaling_policy_configuration = step_cfg;
            existing.target_tracking_scaling_policy_configuration = tt_cfg;
            existing.predictive_scaling_policy_configuration = pred_cfg;
            existing.arn.clone()
        } else {
            let arn = synth_policy_arn(
                &req.account_id,
                &req.region,
                &namespace,
                &resource_id,
                &policy_name,
            );
            let policy = ScalingPolicy {
                arn: arn.clone(),
                policy_name: policy_name.clone(),
                service_namespace: namespace.clone(),
                resource_id: resource_id.clone(),
                scalable_dimension: dimension.clone(),
                policy_type: policy_type.clone(),
                creation_time: Utc::now(),
                step_scaling_policy_configuration: step_cfg,
                target_tracking_scaling_policy_configuration: tt_cfg,
                predictive_scaling_policy_configuration: pred_cfg,
                alarms: Vec::new(),
                last_applied_at: None,
            };
            account.scaling_policies.insert(policy_key, policy);
            arn
        };
        Ok(AwsResponse::ok_json(json!({
            "PolicyARN": arn,
            "Alarms": [],
        })))
    }

    fn describe_scaling_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        validate_service_namespace(&namespace)?;
        let policy_names: Vec<String> = body
            .get("PolicyNames")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let resource_id = body
            .get("ResourceId")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(r) = resource_id.as_deref() {
            validate_resource_id_len("resourceId", r)?;
        }
        let dimension = body
            .get("ScalableDimension")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(d) = dimension.as_deref() {
            validate_scalable_dimension(d)?;
        }
        let max_results = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(50);
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());

        let state = self.state.read();
        let mut all: Vec<ScalingPolicy> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.scaling_policies
                    .values()
                    .filter(|p| p.service_namespace == namespace)
                    .filter(|p| policy_names.is_empty() || policy_names.contains(&p.policy_name))
                    .filter(|p| resource_id.as_deref().is_none_or(|r| p.resource_id == r))
                    .filter(|p| {
                        dimension
                            .as_deref()
                            .is_none_or(|d| p.scalable_dimension == d)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        all.sort_by(|a, b| a.arn.cmp(&b.arn));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let mut response = json!({
            "ScalingPolicies": page.iter().map(scaling_policy_json).collect::<Vec<_>>(),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn delete_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_name = require_str(&body, "PolicyName")?;
        let namespace = require_str(&body, "ServiceNamespace")?;
        let resource_id = require_str(&body, "ResourceId")?;
        let dimension = require_str(&body, "ScalableDimension")?;
        let key = (namespace, resource_id, dimension, policy_name);

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.scaling_policies.remove(&key).is_none() {
            return Err(object_not_found(format!(
                "No scaling policy named {} found for ServiceNamespace={} ResourceId={} ScalableDimension={}",
                key.3, key.0, key.1, key.2
            )));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_scheduled_action(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let action_name = require_str(&body, "ScheduledActionName")?;
        let namespace = require_str(&body, "ServiceNamespace")?;
        let resource_id = require_str(&body, "ResourceId")?;
        let dimension = require_str(&body, "ScalableDimension")?;
        let schedule = body
            .get("Schedule")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let timezone = body
            .get("Timezone")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let start_time = parse_epoch_time(body.get("StartTime"));
        let end_time = parse_epoch_time(body.get("EndTime"));
        let action = body
            .get("ScalableTargetAction")
            .map(parse_scalable_target_action);

        let target_key = (namespace.clone(), resource_id.clone(), dimension.clone());
        let action_key = (
            namespace.clone(),
            resource_id.clone(),
            dimension.clone(),
            action_name.clone(),
        );

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.scalable_targets.contains_key(&target_key) {
            return Err(object_not_found(format!(
                "No scalable target registered for ServiceNamespace={namespace} ResourceId={resource_id} ScalableDimension={dimension}"
            )));
        }
        if let Some(existing) = account.scheduled_actions.get_mut(&action_key) {
            if let Some(s) = schedule {
                existing.schedule = s;
            }
            if timezone.is_some() {
                existing.timezone = timezone;
            }
            if start_time.is_some() {
                existing.start_time = start_time;
            }
            if end_time.is_some() {
                existing.end_time = end_time;
            }
            if action.is_some() {
                existing.scalable_target_action = action;
            }
        } else {
            let schedule = schedule
                .ok_or_else(|| invalid_param("Schedule is required for new scheduled actions"))?;
            let arn = synth_scheduled_action_arn(
                &req.account_id,
                &req.region,
                &namespace,
                &resource_id,
                &action_name,
            );
            let scheduled = ScheduledAction {
                arn,
                scheduled_action_name: action_name.clone(),
                service_namespace: namespace.clone(),
                resource_id: resource_id.clone(),
                scalable_dimension: Some(dimension.clone()),
                schedule,
                timezone,
                start_time,
                end_time,
                scalable_target_action: action,
                creation_time: Utc::now(),
                last_fired_at: None,
            };
            account.scheduled_actions.insert(action_key, scheduled);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_scheduled_actions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        validate_service_namespace(&namespace)?;
        let names: Vec<String> = body
            .get("ScheduledActionNames")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let resource_id = body
            .get("ResourceId")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(r) = resource_id.as_deref() {
            validate_resource_id_len("resourceId", r)?;
        }
        let dimension = body
            .get("ScalableDimension")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(d) = dimension.as_deref() {
            validate_scalable_dimension(d)?;
        }
        let max_results = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(50);
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());

        let state = self.state.read();
        let mut all: Vec<ScheduledAction> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.scheduled_actions
                    .values()
                    .filter(|s| s.service_namespace == namespace)
                    .filter(|s| names.is_empty() || names.contains(&s.scheduled_action_name))
                    .filter(|s| resource_id.as_deref().is_none_or(|r| s.resource_id == r))
                    .filter(|s| {
                        dimension
                            .as_deref()
                            .is_none_or(|d| s.scalable_dimension.as_deref() == Some(d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        all.sort_by(|a, b| a.arn.cmp(&b.arn));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let mut response = json!({
            "ScheduledActions": page.iter().map(scheduled_action_json).collect::<Vec<_>>(),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn delete_scheduled_action(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        let action_name = require_str(&body, "ScheduledActionName")?;
        let resource_id = require_str(&body, "ResourceId")?;
        let dimension = require_str(&body, "ScalableDimension")?;
        let key = (namespace, resource_id, dimension, action_name);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.scheduled_actions.remove(&key).is_none() {
            return Err(object_not_found(format!(
                "No scheduled action named {} found for ServiceNamespace={} ResourceId={} ScalableDimension={}",
                key.3, key.0, key.1, key.2
            )));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_scaling_activities(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        validate_service_namespace(&namespace)?;
        let resource_id = body
            .get("ResourceId")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(r) = resource_id.as_deref() {
            validate_resource_id_len("resourceId", r)?;
        }
        let dimension = body
            .get("ScalableDimension")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(d) = dimension.as_deref() {
            validate_scalable_dimension(d)?;
        }
        let include_not_scaled = body
            .get("IncludeNotScaledActivities")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let max_results = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(50);
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());

        let state = self.state.read();
        let mut all: Vec<ScalingActivity> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.scaling_activities
                    .iter()
                    .filter(|act| act.service_namespace == namespace)
                    .filter(|act| resource_id.as_deref().is_none_or(|r| act.resource_id == r))
                    .filter(|act| {
                        dimension
                            .as_deref()
                            .is_none_or(|d| act.scalable_dimension == d)
                    })
                    .filter(|act| include_not_scaled || act.status_code != "Failed")
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        all.sort_by_key(|a| std::cmp::Reverse(a.start_time));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let mut response = json!({
            "ScalingActivities": page.iter().map(scaling_activity_json).collect::<Vec<_>>(),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn get_predictive_scaling_forecast(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let namespace = require_str(&body, "ServiceNamespace")?;
        let resource_id = require_str(&body, "ResourceId")?;
        let dimension = require_str(&body, "ScalableDimension")?;
        let policy_name = require_str(&body, "PolicyName")?;
        let start = parse_epoch_time(body.get("StartTime"))
            .ok_or_else(|| invalid_param("StartTime is required"))?;
        let end = parse_epoch_time(body.get("EndTime"))
            .ok_or_else(|| invalid_param("EndTime is required"))?;

        let policy_key = (
            namespace.clone(),
            resource_id.clone(),
            dimension.clone(),
            policy_name.clone(),
        );
        let state = self.state.read();
        let policy = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.scaling_policies.get(&policy_key))
            .ok_or_else(|| {
                // GetPredictiveScalingForecast's Smithy model only declares
                // InternalServiceException and ValidationException. Missing
                // policies surface as ValidationException, not the
                // ObjectNotFoundException used by the Delete/Put ops.
                invalid_param(format!(
                    "No predictive scaling policy named {policy_name} found for ServiceNamespace={namespace} ResourceId={resource_id} ScalableDimension={dimension}"
                ))
            })?;
        if policy.policy_type != "PredictiveScaling" {
            return Err(invalid_param(
                "Policy is not a PredictiveScaling policy; cannot return a forecast",
            ));
        }
        let buckets = synth_forecast(start, end);
        Ok(AwsResponse::ok_json(json!({
            "LoadForecast": [{
                "Timestamps": buckets.iter().map(|(t, _)| t.timestamp() as f64).collect::<Vec<_>>(),
                "Values": buckets.iter().map(|(_, v)| *v as f64).collect::<Vec<_>>(),
                "MetricSpecification": {
                    "TargetValue": 70.0,
                    "PredefinedMetricPairSpecification": {
                        "PredefinedMetricType": "ECSServiceCPUUtilization"
                    }
                },
            }],
            "CapacityForecast": {
                "Timestamps": buckets.iter().map(|(t, _)| t.timestamp() as f64).collect::<Vec<_>>(),
                "Values": buckets
                    .iter()
                    .map(|(_, v)| ((*v as f64) / 100.0).ceil().max(1.0))
                    .collect::<Vec<_>>(),
            },
            "UpdateTime": Utc::now().timestamp() as f64,
        })))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require_str(&body, "ResourceARN")?;
        let state = self.state.read();
        let account = state.accounts.get(&req.account_id);
        // Real AWS rejects unknown ARNs with ResourceNotFoundException rather
        // than returning an empty tag set — match that so callers can tell
        // a missing target apart from a target with no tags. The Smithy
        // model for the tag-* ops declares ResourceNotFoundException, not the
        // ObjectNotFoundException used by the Delete/Put scaling-target ops.
        let exists = account.is_some_and(|a| resource_exists(a, &arn));
        if !exists {
            return Err(resource_not_found(format!("Resource {arn} not found")));
        }
        let tags = account
            .and_then(|a| a.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require_str(&body, "ResourceARN")?;
        let tags_in = body
            .get("Tags")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid_param("Tags is required"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !resource_exists(account, &arn) {
            return Err(resource_not_found(format!("Resource {arn} not found")));
        }
        let entry = account.tags.entry(arn).or_default();
        for (k, v) in tags_in {
            if let Some(s) = v.as_str() {
                entry.insert(k.clone(), s.to_string());
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require_str(&body, "ResourceARN")?;
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !resource_exists(account, &arn) {
            return Err(resource_not_found(format!("Resource {arn} not found")));
        }
        if let Some(tags) = account.tags.get_mut(&arn) {
            for k in keys {
                tags.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}

fn account_mut<'a>(
    state: &'a mut ApplicationAutoScalingAccounts,
    account_id: &str,
) -> &'a mut AccountState {
    state.accounts.entry(account_id.to_string()).or_default()
}

fn require_str(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| invalid_param(format!("{field} is required")))
}

fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

// Smithy: com.amazonaws.applicationautoscaling#ServiceNamespace
const VALID_SERVICE_NAMESPACES: &[&str] = &[
    "ecs",
    "elasticmapreduce",
    "ec2",
    "appstream",
    "dynamodb",
    "rds",
    "sagemaker",
    "custom-resource",
    "comprehend",
    "lambda",
    "cassandra",
    "kafka",
    "elasticache",
    "neptune",
    "workspaces",
];

// Smithy: com.amazonaws.applicationautoscaling#ScalableDimension
const VALID_SCALABLE_DIMENSIONS: &[&str] = &[
    "ecs:service:DesiredCount",
    "ec2:spot-fleet-request:TargetCapacity",
    "elasticmapreduce:instancegroup:InstanceCount",
    "appstream:fleet:DesiredCapacity",
    "dynamodb:table:ReadCapacityUnits",
    "dynamodb:table:WriteCapacityUnits",
    "dynamodb:index:ReadCapacityUnits",
    "dynamodb:index:WriteCapacityUnits",
    "rds:cluster:ReadReplicaCount",
    "sagemaker:variant:DesiredInstanceCount",
    "custom-resource:ResourceType:Property",
    "comprehend:document-classifier-endpoint:DesiredInferenceUnits",
    "comprehend:entity-recognizer-endpoint:DesiredInferenceUnits",
    "lambda:function:ProvisionedConcurrency",
    "cassandra:table:ReadCapacityUnits",
    "cassandra:table:WriteCapacityUnits",
    "kafka:broker-storage:VolumeSize",
    "elasticache:cache-cluster:Nodes",
    "elasticache:replication-group:NodeGroups",
    "elasticache:replication-group:Replicas",
    "neptune:cluster:ReadReplicaCount",
    "sagemaker:variant:DesiredProvisionedConcurrency",
    "sagemaker:inference-component:DesiredCopyCount",
    "workspaces:workspacespool:DesiredUserSessions",
];

fn validate_service_namespace(value: &str) -> Result<(), AwsServiceError> {
    if VALID_SERVICE_NAMESPACES.contains(&value) {
        Ok(())
    } else {
        Err(invalid_param(format!(
            "Value '{value}' at 'serviceNamespace' failed to satisfy constraint: Member must satisfy enum value set: {VALID_SERVICE_NAMESPACES:?}"
        )))
    }
}

fn validate_scalable_dimension(value: &str) -> Result<(), AwsServiceError> {
    if VALID_SCALABLE_DIMENSIONS.contains(&value) {
        Ok(())
    } else {
        Err(invalid_param(format!(
            "Value '{value}' at 'scalableDimension' failed to satisfy constraint: Member must satisfy enum value set: {VALID_SCALABLE_DIMENSIONS:?}"
        )))
    }
}

// Smithy: com.amazonaws.applicationautoscaling#ResourceIdMaxLen1600 (length 1..=1600).
// Same shape is also used for RoleARN, so we validate both via the same range.
fn validate_resource_id_len(field: &str, value: &str) -> Result<(), AwsServiceError> {
    let len = value.chars().count();
    if !(1..=1600).contains(&len) {
        return Err(invalid_param(format!(
            "Value at '{field}' failed to satisfy constraint: Member must have length between 1 and 1600, inclusive"
        )));
    }
    Ok(())
}

fn object_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ObjectNotFoundException", msg)
}

fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}

fn parse_suspended_state(value: &Value) -> SuspendedState {
    SuspendedState {
        dynamic_scaling_in_suspended: value
            .get("DynamicScalingInSuspended")
            .and_then(Value::as_bool),
        dynamic_scaling_out_suspended: value
            .get("DynamicScalingOutSuspended")
            .and_then(Value::as_bool),
        scheduled_scaling_suspended: value
            .get("ScheduledScalingSuspended")
            .and_then(Value::as_bool),
    }
}

fn parse_scalable_target_action(value: &Value) -> ScalableTargetAction {
    ScalableTargetAction {
        min_capacity: value
            .get("MinCapacity")
            .and_then(Value::as_i64)
            .map(|n| n as i32),
        max_capacity: value
            .get("MaxCapacity")
            .and_then(Value::as_i64)
            .map(|n| n as i32),
    }
}

fn parse_epoch_time(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let v = value?;
    if let Some(n) = v.as_f64() {
        return DateTime::<Utc>::from_timestamp(
            n.trunc() as i64,
            ((n.fract() * 1e9) as u32).min(999_999_999),
        );
    }
    if let Some(s) = v.as_str() {
        return DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc));
    }
    None
}

fn resource_exists(account: &AccountState, arn: &str) -> bool {
    account.scalable_targets.values().any(|t| t.arn == arn)
        || account.scaling_policies.values().any(|p| p.arn == arn)
}

fn synth_activity_id() -> String {
    Uuid::new_v4().to_string()
}

fn synth_scalable_target_arn(account_id: &str, region: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    let id = Uuid::new_v4().simple().to_string();
    let id = &id[..10];
    Arn::new(
        "application-autoscaling",
        region,
        account_id,
        &format!("scalable-target/{id}"),
    )
    .with_partition(partition_for(region))
    .to_string()
}

fn synth_policy_arn(
    account_id: &str,
    region: &str,
    namespace: &str,
    resource_id: &str,
    name: &str,
) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    let id = Uuid::new_v4();
    format!(
        "arn:aws:autoscaling:{region}:{account_id}:scalingPolicy:{id}:resource/{namespace}/{resource_id}:policyName/{name}"
    )
}

fn synth_scheduled_action_arn(
    account_id: &str,
    region: &str,
    namespace: &str,
    resource_id: &str,
    name: &str,
) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    let id = Uuid::new_v4();
    format!(
        "arn:aws:autoscaling:{region}:{account_id}:scheduledAction:{id}:resource/{namespace}/{resource_id}:scheduledActionName/{name}"
    )
}

fn default_service_linked_role(account_id: &str, namespace: &str) -> String {
    let suffix = match namespace {
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
    Arn::global(
        "iam",
        account_id,
        &format!("role/aws-service-role/applicationautoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_{suffix}"),
    )
    .to_string()
}

fn synth_forecast(start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<(DateTime<Utc>, i32)> {
    let mut out = Vec::new();
    if end <= start {
        return out;
    }
    let mut cursor = start;
    let step = Duration::hours(1);
    while cursor < end {
        // Sine-ish curve scaled into 30..=90 percent CPU range, deterministic
        // by hour-of-day so tests can pin specific values.
        let h = cursor.timestamp().rem_euclid(86_400) / 3600;
        let v = 30 + ((h * 5) as i32 % 60).abs();
        out.push((cursor, v));
        cursor += step;
        if out.len() >= 168 {
            break; // cap at one week of hourly buckets
        }
    }
    out
}

// ─── JSON shaping ────────────────────────────────────────────────────

fn scalable_target_json(t: &ScalableTarget) -> Value {
    let mut obj = json!({
        "ScalableTargetARN": t.arn,
        "ServiceNamespace": t.service_namespace,
        "ResourceId": t.resource_id,
        "ScalableDimension": t.scalable_dimension,
        "MinCapacity": t.min_capacity,
        "MaxCapacity": t.max_capacity,
        "RoleARN": t.role_arn,
        "CreationTime": t.creation_time.timestamp() as f64,
    });
    if let Some(s) = &t.suspended_state {
        obj.as_object_mut().unwrap().insert(
            "SuspendedState".to_string(),
            json!({
                "DynamicScalingInSuspended": s.dynamic_scaling_in_suspended,
                "DynamicScalingOutSuspended": s.dynamic_scaling_out_suspended,
                "ScheduledScalingSuspended": s.scheduled_scaling_suspended,
            }),
        );
    }
    if let Some(c) = t.predicted_capacity {
        obj.as_object_mut()
            .unwrap()
            .insert("PredictedCapacity".to_string(), json!(c));
    }
    obj
}

fn scaling_policy_json(p: &ScalingPolicy) -> Value {
    let mut obj = json!({
        "PolicyARN": p.arn,
        "PolicyName": p.policy_name,
        "ServiceNamespace": p.service_namespace,
        "ResourceId": p.resource_id,
        "ScalableDimension": p.scalable_dimension,
        "PolicyType": p.policy_type,
        "CreationTime": p.creation_time.timestamp() as f64,
        "Alarms": p.alarms.iter().map(|a| json!({
            "AlarmName": a.alarm_name,
            "AlarmARN": a.alarm_arn,
        })).collect::<Vec<_>>(),
    });
    if let Some(c) = &p.step_scaling_policy_configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("StepScalingPolicyConfiguration".to_string(), c.clone());
    }
    if let Some(c) = &p.target_tracking_scaling_policy_configuration {
        obj.as_object_mut().unwrap().insert(
            "TargetTrackingScalingPolicyConfiguration".to_string(),
            c.clone(),
        );
    }
    if let Some(c) = &p.predictive_scaling_policy_configuration {
        obj.as_object_mut().unwrap().insert(
            "PredictiveScalingPolicyConfiguration".to_string(),
            c.clone(),
        );
    }
    obj
}

fn scheduled_action_json(s: &ScheduledAction) -> Value {
    let mut obj = json!({
        "ScheduledActionARN": s.arn,
        "ScheduledActionName": s.scheduled_action_name,
        "ServiceNamespace": s.service_namespace,
        "ResourceId": s.resource_id,
        "Schedule": s.schedule,
        "CreationTime": s.creation_time.timestamp() as f64,
    });
    if let Some(d) = &s.scalable_dimension {
        obj.as_object_mut()
            .unwrap()
            .insert("ScalableDimension".to_string(), Value::String(d.clone()));
    }
    if let Some(t) = &s.timezone {
        obj.as_object_mut()
            .unwrap()
            .insert("Timezone".to_string(), Value::String(t.clone()));
    }
    if let Some(t) = s.start_time {
        obj.as_object_mut()
            .unwrap()
            .insert("StartTime".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = s.end_time {
        obj.as_object_mut()
            .unwrap()
            .insert("EndTime".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(a) = &s.scalable_target_action {
        let mut action = serde_json::Map::new();
        if let Some(min) = a.min_capacity {
            action.insert("MinCapacity".to_string(), json!(min));
        }
        if let Some(max) = a.max_capacity {
            action.insert("MaxCapacity".to_string(), json!(max));
        }
        obj.as_object_mut()
            .unwrap()
            .insert("ScalableTargetAction".to_string(), Value::Object(action));
    }
    obj
}

fn scaling_activity_json(a: &ScalingActivity) -> Value {
    let mut obj = json!({
        "ActivityId": a.activity_id,
        "ServiceNamespace": a.service_namespace,
        "ResourceId": a.resource_id,
        "ScalableDimension": a.scalable_dimension,
        "Description": a.description,
        "Cause": a.cause,
        "StartTime": a.start_time.timestamp() as f64,
        "StatusCode": a.status_code,
    });
    if let Some(t) = a.end_time {
        obj.as_object_mut()
            .unwrap()
            .insert("EndTime".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(m) = &a.status_message {
        obj.as_object_mut()
            .unwrap()
            .insert("StatusMessage".to_string(), Value::String(m.clone()));
    }
    if let Some(d) = &a.details {
        obj.as_object_mut()
            .unwrap()
            .insert("Details".to_string(), Value::String(d.clone()));
    }
    if !a.not_scaled_reasons.is_empty() {
        let arr: Vec<Value> = a
            .not_scaled_reasons
            .iter()
            .map(not_scaled_reason_json)
            .collect();
        obj.as_object_mut()
            .unwrap()
            .insert("NotScaledReasons".to_string(), Value::Array(arr));
    }
    obj
}

fn not_scaled_reason_json(r: &NotScaledReason) -> Value {
    let mut obj = json!({ "Code": r.code });
    if let Some(v) = r.max_capacity {
        obj.as_object_mut()
            .unwrap()
            .insert("MaxCapacity".to_string(), json!(v));
    }
    if let Some(v) = r.min_capacity {
        obj.as_object_mut()
            .unwrap()
            .insert("MinCapacity".to_string(), json!(v));
    }
    if let Some(v) = r.current_capacity {
        obj.as_object_mut()
            .unwrap()
            .insert("CurrentCapacity".to_string(), json!(v));
    }
    obj
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use std::collections::HashMap;

    fn make_req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "application-autoscaling".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn register_scalable_target_emits_scaling_activity() {
        let svc = ApplicationAutoScalingService::default();
        let req = make_req(
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/cluster1/svc1",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 1,
                "MaxCapacity": 5,
            }),
        );
        svc.register_scalable_target(&req).unwrap();
        let state = svc.state.read();
        let activities = &state
            .accounts
            .get("123456789012")
            .unwrap()
            .scaling_activities;
        assert_eq!(
            activities.len(),
            1,
            "expected 1 activity, got {activities:?}"
        );
        let a = &activities[0];
        assert_eq!(a.service_namespace, "ecs");
        assert_eq!(a.status_code, "Successful");
        assert!(a.description.contains("min capacity to 1"));
        assert!(a.description.contains("max capacity to 5"));
    }

    #[test]
    fn updating_bounds_appends_another_activity() {
        let svc = ApplicationAutoScalingService::default();
        let initial = make_req(
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/cluster1/svc1",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 1,
                "MaxCapacity": 5,
            }),
        );
        svc.register_scalable_target(&initial).unwrap();
        let update = make_req(
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/cluster1/svc1",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 2,
                "MaxCapacity": 10,
            }),
        );
        svc.register_scalable_target(&update).unwrap();
        let state = svc.state.read();
        let activities = &state
            .accounts
            .get("123456789012")
            .unwrap()
            .scaling_activities;
        assert_eq!(activities.len(), 2);
        assert!(activities[1].description.contains("Updated"));
    }

    #[test]
    fn re_register_with_same_bounds_does_not_log_activity() {
        let svc = ApplicationAutoScalingService::default();
        let req = make_req(
            "RegisterScalableTarget",
            json!({
                "ServiceNamespace": "ecs",
                "ResourceId": "service/cluster1/svc1",
                "ScalableDimension": "ecs:service:DesiredCount",
                "MinCapacity": 1,
                "MaxCapacity": 5,
            }),
        );
        svc.register_scalable_target(&req).unwrap();
        // Same bounds again -> no new activity row.
        svc.register_scalable_target(&req).unwrap();
        let state = svc.state.read();
        let activities = &state
            .accounts
            .get("123456789012")
            .unwrap()
            .scaling_activities;
        assert_eq!(activities.len(), 1);
    }

    // bug-audit 2026-05-28, 1.7: list ops reject a malformed NextToken
    // (paginate_checked -> ValidationException) instead of silently restarting
    // at page 0. The rejection primitive is exercised here; the wiring lives in
    // the Describe* paginated handlers above.
    #[test]
    fn paginate_checked_rejects_invalid_token() {
        use fakecloud_core::pagination::paginate_checked;
        let items: Vec<i32> = (0..5).collect();
        assert!(paginate_checked(&items, Some("not-a-valid-token"), 3).is_err());
        assert!(paginate_checked(&items, Some("2"), 3).is_ok());
        assert!(paginate_checked(&items, None, 3).is_ok());
    }
}
