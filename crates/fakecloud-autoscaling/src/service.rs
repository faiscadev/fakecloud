//! EC2 Auto Scaling (`autoscaling`) Query-protocol service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    AccountState, AsgInstance, AsgTag, AutoScalingGroup, AutoScalingSnapshot, LaunchConfiguration,
    LaunchTemplateSpec, ScalingActivity, SharedAutoScalingState,
    AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
};

const NS: &str = "http://autoscaling.amazonaws.com/doc/2011-01-01/";

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateLaunchConfiguration",
    "DescribeLaunchConfigurations",
    "DeleteLaunchConfiguration",
    "CreateAutoScalingGroup",
    "DescribeAutoScalingGroups",
    "UpdateAutoScalingGroup",
    "DeleteAutoScalingGroup",
    "SetDesiredCapacity",
    "DescribeAutoScalingInstances",
    "DescribeScalingActivities",
    "CreateOrUpdateTags",
    "DeleteTags",
    "DescribeTags",
];

pub struct AutoScalingService {
    state: SharedAutoScalingState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl AutoScalingService {
    pub fn new(state: SharedAutoScalingState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let bytes = {
            let snap = AutoScalingSnapshot {
                schema_version: AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
                accounts: Some(self.state.read().clone()),
            };
            serde_json::to_vec(&snap).unwrap_or_default()
        };
        let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
    }
}

fn xesc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

fn el(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", xesc(value))
}

/// Parse `Prefix.member.N` (and the `Prefix.N` variant some SDKs emit) into a list.
fn member_list(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for n in 1..=200 {
        let v = req
            .query_params
            .get(&format!("{prefix}.member.{n}"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}")));
        match v {
            Some(val) => out.push(val.clone()),
            None => break,
        }
    }
    out
}

fn iso(t: chrono::DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn gen_instance_id() -> String {
    let hex = Uuid::new_v4().simple().to_string();
    format!("i-{}", &hex[..17])
}

impl AutoScalingService {
    fn arn(&self, account: &str, region: &str, kind: &str, name: &str) -> String {
        // e.g. arn:aws:autoscaling:us-east-1:123:autoScalingGroup:<uuid>:autoScalingGroupName/<name>
        format!(
            "arn:aws:autoscaling:{region}:{account}:{kind}:{}:{kind}Name/{name}",
            Uuid::new_v4()
        )
    }

    fn create_launch_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "LaunchConfigurationName")?;
        let image_id = required_query_param(req, "ImageId")?;
        let instance_type = required_query_param(req, "InstanceType")?;
        let lc = LaunchConfiguration {
            arn: self.arn(&req.account_id, &req.region, "launchConfiguration", &name),
            name: name.clone(),
            image_id,
            instance_type,
            key_name: optional_query_param(req, "KeyName"),
            security_groups: member_list(req, "SecurityGroups"),
            user_data: optional_query_param(req, "UserData"),
            iam_instance_profile: optional_query_param(req, "IamInstanceProfile"),
            associate_public_ip_address: optional_query_param(req, "AssociatePublicIpAddress")
                .map(|v| v == "true"),
            created_time: Utc::now(),
        };
        {
            let mut accounts = self.state.write();
            accounts
                .get_or_create(&req.account_id)
                .launch_configurations
                .insert(name, lc);
        }
        Ok(self.ok("CreateLaunchConfiguration", String::new(), req))
    }

    fn describe_launch_configurations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let wanted = member_list(req, "LaunchConfigurationNames");
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        let items: String = st
            .launch_configurations
            .values()
            .filter(|lc| wanted.is_empty() || wanted.contains(&lc.name))
            .map(|lc| {
                format!(
                    "<member>{}{}{}{}{}{}</member>",
                    el("LaunchConfigurationName", &lc.name),
                    el("LaunchConfigurationARN", &lc.arn),
                    el("ImageId", &lc.image_id),
                    el("InstanceType", &lc.instance_type),
                    lc.key_name
                        .as_deref()
                        .map(|k| el("KeyName", k))
                        .unwrap_or_default(),
                    el("CreatedTime", &iso(lc.created_time)),
                )
            })
            .collect();
        let inner = format!("<LaunchConfigurations>{items}</LaunchConfigurations>");
        Ok(self.ok("DescribeLaunchConfigurations", inner, req))
    }

    fn delete_launch_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "LaunchConfigurationName")?;
        let mut accounts = self.state.write();
        accounts
            .get_or_create(&req.account_id)
            .launch_configurations
            .remove(&name);
        Ok(self.ok("DeleteLaunchConfiguration", String::new(), req))
    }

    fn create_auto_scaling_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        let min_size = required_query_param(req, "MinSize")?
            .parse::<i64>()
            .unwrap_or(0);
        let max_size = required_query_param(req, "MaxSize")?
            .parse::<i64>()
            .unwrap_or(0);
        let desired = optional_query_param(req, "DesiredCapacity")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(min_size);

        let launch_template = parse_launch_template(req);
        let launch_configuration_name = optional_query_param(req, "LaunchConfigurationName");
        if launch_configuration_name.is_none() && launch_template.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "Valid requests must contain either LaunchTemplate, LaunchConfigurationName, \
                 InstanceId or MixedInstancesPolicy parameter.",
            ));
        }

        let mut azs = member_list(req, "AvailabilityZones");
        let vpc_zone_identifier = optional_query_param(req, "VPCZoneIdentifier");
        if azs.is_empty() {
            azs.push(format!("{}a", req.region));
        }

        let tags = parse_tags(req);
        let mut group = AutoScalingGroup {
            arn: self.arn(&req.account_id, &req.region, "autoScalingGroup", &name),
            name: name.clone(),
            launch_configuration_name,
            launch_template,
            min_size,
            max_size,
            desired_capacity: desired,
            default_cooldown: optional_query_param(req, "DefaultCooldown")
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            availability_zones: azs.clone(),
            vpc_zone_identifier,
            health_check_type: optional_query_param(req, "HealthCheckType")
                .unwrap_or_else(|| "EC2".to_string()),
            health_check_grace_period: optional_query_param(req, "HealthCheckGracePeriod")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            target_group_arns: member_list(req, "TargetGroupARNs"),
            load_balancer_names: member_list(req, "LoadBalancerNames"),
            new_instances_protected_from_scale_in: optional_query_param(
                req,
                "NewInstancesProtectedFromScaleIn",
            )
            .map(|v| v == "true")
            .unwrap_or(false),
            created_time: Utc::now(),
            instances: Vec::new(),
            tags,
            status: None,
        };

        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            // Reconcile to the desired capacity (batch 1: metadata-only
            // instances so the Terraform create waiter — which blocks until the
            // ASG has `desired_capacity` InService instances — completes).
            reconcile_capacity(st, &mut group, &azs);
            st.groups.insert(name, group);
        }
        Ok(self.ok("CreateAutoScalingGroup", String::new(), req))
    }

    fn update_auto_scaling_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let azs = {
            let Some(g) = st.groups.get(&name) else {
                return Err(group_not_found(&name));
            };
            g.availability_zones.clone()
        };
        let mut group = st.groups.get(&name).cloned().unwrap();
        if let Some(v) = optional_query_param(req, "MinSize").and_then(|v| v.parse().ok()) {
            group.min_size = v;
        }
        if let Some(v) = optional_query_param(req, "MaxSize").and_then(|v| v.parse().ok()) {
            group.max_size = v;
        }
        if let Some(v) = optional_query_param(req, "DesiredCapacity").and_then(|v| v.parse().ok()) {
            group.desired_capacity = v;
        }
        if let Some(v) = optional_query_param(req, "LaunchConfigurationName") {
            group.launch_configuration_name = Some(v);
        }
        if let Some(v) = optional_query_param(req, "HealthCheckType") {
            group.health_check_type = v;
        }
        reconcile_capacity(st, &mut group, &azs);
        st.groups.insert(name, group);
        drop(accounts);
        Ok(self.ok("UpdateAutoScalingGroup", String::new(), req))
    }

    fn set_desired_capacity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        let desired = required_query_param(req, "DesiredCapacity")?
            .parse::<i64>()
            .unwrap_or(0);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(mut group) = st.groups.get(&name).cloned() else {
            return Err(group_not_found(&name));
        };
        let azs = group.availability_zones.clone();
        group.desired_capacity = desired;
        reconcile_capacity(st, &mut group, &azs);
        st.groups.insert(name, group);
        drop(accounts);
        Ok(self.ok("SetDesiredCapacity", String::new(), req))
    }

    fn delete_auto_scaling_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        let force = optional_query_param(req, "ForceDelete")
            .map(|v| v == "true")
            .unwrap_or(false);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(g) = st.groups.get(&name) {
            if !g.instances.is_empty() && !force {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceInUse",
                    format!(
                        "You cannot delete an AutoScalingGroup while there are instances or \
                         pending Spot instance requests still in the group. ({name})"
                    ),
                ));
            }
        }
        st.groups.remove(&name);
        Ok(self.ok("DeleteAutoScalingGroup", String::new(), req))
    }

    fn describe_auto_scaling_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let wanted = member_list(req, "AutoScalingGroupNames");
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        let items: String = st
            .groups
            .values()
            .filter(|g| wanted.is_empty() || wanted.contains(&g.name))
            .map(group_xml)
            .collect();
        let inner = format!("<AutoScalingGroups>{items}</AutoScalingGroups>");
        Ok(self.ok("DescribeAutoScalingGroups", inner, req))
    }

    fn describe_auto_scaling_instances(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let wanted = member_list(req, "InstanceIds");
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        let items: String = st
            .groups
            .values()
            .flat_map(|g| g.instances.iter().map(move |i| (g, i)))
            .filter(|(_, i)| wanted.is_empty() || wanted.contains(&i.instance_id))
            .map(|(g, i)| asg_instance_member(g, i, true))
            .collect();
        let inner = format!("<AutoScalingInstances>{items}</AutoScalingInstances>");
        Ok(self.ok("DescribeAutoScalingInstances", inner, req))
    }

    fn describe_scaling_activities(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group = optional_query_param(req, "AutoScalingGroupName");
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        let items: String = st
            .activities
            .iter()
            .filter(|a| {
                group
                    .as_ref()
                    .map(|g| &a.auto_scaling_group_name == g)
                    .unwrap_or(true)
            })
            .map(activity_member)
            .collect();
        let inner = format!("<Activities>{items}</Activities>");
        Ok(self.ok("DescribeScalingActivities", inner, req))
    }

    fn create_or_update_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // The standard form is Tags.member.N.{ResourceId,Key,Value,PropagateAtLaunch}.
        for n in 1..=200 {
            let rid = req.query_params.get(&format!("Tags.member.{n}.ResourceId"));
            let Some(rid) = rid else { break };
            let key = req
                .query_params
                .get(&format!("Tags.member.{n}.Key"))
                .cloned()
                .unwrap_or_default();
            let value = req
                .query_params
                .get(&format!("Tags.member.{n}.Value"))
                .cloned()
                .unwrap_or_default();
            let prop = req
                .query_params
                .get(&format!("Tags.member.{n}.PropagateAtLaunch"))
                .map(|v| v == "true")
                .unwrap_or(false);
            if let Some(g) = st.groups.get_mut(rid) {
                g.tags.retain(|t| t.key != key);
                g.tags.push(AsgTag {
                    key,
                    value,
                    propagate_at_launch: prop,
                });
            }
        }
        drop(accounts);
        Ok(self.ok("CreateOrUpdateTags", String::new(), req))
    }

    fn delete_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for n in 1..=200 {
            let rid = req.query_params.get(&format!("Tags.member.{n}.ResourceId"));
            let Some(rid) = rid else { break };
            let key = req
                .query_params
                .get(&format!("Tags.member.{n}.Key"))
                .cloned()
                .unwrap_or_default();
            if let Some(g) = st.groups.get_mut(rid) {
                g.tags.retain(|t| t.key != key);
            }
        }
        drop(accounts);
        Ok(self.ok("DeleteTags", String::new(), req))
    }

    fn describe_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        let items: String = st
            .groups
            .values()
            .flat_map(|g| {
                g.tags.iter().map(move |t| {
                    format!(
                        "<member>{}{}{}{}{}</member>",
                        el("ResourceId", &g.name),
                        el("ResourceType", "auto-scaling-group"),
                        el("Key", &t.key),
                        el("Value", &t.value),
                        el("PropagateAtLaunch", &t.propagate_at_launch.to_string()),
                    )
                })
            })
            .collect();
        let inner = format!("<Tags>{items}</Tags>");
        Ok(self.ok("DescribeTags", inner, req))
    }

    fn ok(&self, action: &str, inner: String, req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(action, NS, &inner, &req.request_id),
        )
    }
}

fn group_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationError",
        format!("AutoScalingGroup name not found - AutoScalingGroup {name} not found"),
    )
}

fn parse_launch_template(req: &AwsRequest) -> Option<LaunchTemplateSpec> {
    let id = optional_query_param(req, "LaunchTemplate.LaunchTemplateId");
    let name = optional_query_param(req, "LaunchTemplate.LaunchTemplateName");
    if id.is_none() && name.is_none() {
        return None;
    }
    Some(LaunchTemplateSpec {
        launch_template_id: id,
        launch_template_name: name,
        version: optional_query_param(req, "LaunchTemplate.Version"),
    })
}

fn parse_tags(req: &AwsRequest) -> Vec<AsgTag> {
    let mut out = Vec::new();
    for n in 1..=200 {
        let key = req.query_params.get(&format!("Tags.member.{n}.Key"));
        let Some(key) = key else { break };
        out.push(AsgTag {
            key: key.clone(),
            value: req
                .query_params
                .get(&format!("Tags.member.{n}.Value"))
                .cloned()
                .unwrap_or_default(),
            propagate_at_launch: req
                .query_params
                .get(&format!("Tags.member.{n}.PropagateAtLaunch"))
                .map(|v| v == "true")
                .unwrap_or(false),
        });
    }
    out
}

/// Bring a group's instance set to its desired capacity. Batch 1 launches
/// metadata-only instances (so Describe reports `desired_capacity` InService
/// instances and the Terraform create waiter completes) and records a
/// Successful scaling activity for each launch/termination. Batch 2 replaces
/// the synthetic ids with real container-backed EC2 instances.
fn reconcile_capacity(st: &mut AccountState, group: &mut AutoScalingGroup, azs: &[String]) {
    let target = group.desired_capacity.max(0) as usize;
    let default_az = azs
        .first()
        .cloned()
        .unwrap_or_else(|| "us-east-1a".to_string());
    while group.instances.len() < target {
        let az = azs
            .get(group.instances.len() % azs.len().max(1))
            .cloned()
            .unwrap_or_else(|| default_az.clone());
        let id = gen_instance_id();
        group.instances.push(AsgInstance {
            instance_id: id.clone(),
            availability_zone: az,
            lifecycle_state: "InService".to_string(),
            health_status: "Healthy".to_string(),
            launch_configuration_name: group.launch_configuration_name.clone(),
            protected_from_scale_in: group.new_instances_protected_from_scale_in,
        });
        st.activities.insert(
            0,
            activity(&group.name, &format!("Launching a new EC2 instance: {id}")),
        );
    }
    while group.instances.len() > target {
        if let Some(removed) = group.instances.pop() {
            st.activities.insert(
                0,
                activity(
                    &group.name,
                    &format!("Terminating EC2 instance: {}", removed.instance_id),
                ),
            );
        }
    }
}

fn activity(group: &str, description: &str) -> ScalingActivity {
    let now = Utc::now();
    ScalingActivity {
        activity_id: Uuid::new_v4().to_string(),
        auto_scaling_group_name: group.to_string(),
        description: description.to_string(),
        cause: "a user request".to_string(),
        start_time: now,
        end_time: Some(now),
        status_code: "Successful".to_string(),
        progress: 100,
        details: String::new(),
    }
}

fn group_xml(g: &AutoScalingGroup) -> String {
    let instances: String = g
        .instances
        .iter()
        .map(|i| asg_instance_member(g, i, false))
        .collect();
    let azs: String = g
        .availability_zones
        .iter()
        .map(|a| format!("<member>{}</member>", xesc(a)))
        .collect();
    let tgs: String = g
        .target_group_arns
        .iter()
        .map(|a| format!("<member>{}</member>", xesc(a)))
        .collect();
    let tags: String = g
        .tags
        .iter()
        .map(|t| {
            format!(
                "<member>{}{}{}{}{}</member>",
                el("ResourceId", &g.name),
                el("ResourceType", "auto-scaling-group"),
                el("Key", &t.key),
                el("Value", &t.value),
                el("PropagateAtLaunch", &t.propagate_at_launch.to_string()),
            )
        })
        .collect();
    format!(
        "<member>{}{}{}{}{}{}{}{}{}{}{}<AvailabilityZones>{azs}</AvailabilityZones>\
         <Instances>{instances}</Instances><TargetGroupARNs>{tgs}</TargetGroupARNs>\
         <Tags>{tags}</Tags>{}</member>",
        el("AutoScalingGroupName", &g.name),
        el("AutoScalingGroupARN", &g.arn),
        g.launch_configuration_name
            .as_deref()
            .map(|n| el("LaunchConfigurationName", n))
            .unwrap_or_default(),
        el("MinSize", &g.min_size.to_string()),
        el("MaxSize", &g.max_size.to_string()),
        el("DesiredCapacity", &g.desired_capacity.to_string()),
        el("DefaultCooldown", &g.default_cooldown.to_string()),
        el("HealthCheckType", &g.health_check_type),
        el(
            "HealthCheckGracePeriod",
            &g.health_check_grace_period.to_string()
        ),
        el("CreatedTime", &iso(g.created_time)),
        g.vpc_zone_identifier
            .as_deref()
            .map(|v| el("VPCZoneIdentifier", v))
            .unwrap_or_default(),
        el(
            "NewInstancesProtectedFromScaleIn",
            &g.new_instances_protected_from_scale_in.to_string()
        ),
    )
}

fn asg_instance_member(g: &AutoScalingGroup, i: &AsgInstance, with_group: bool) -> String {
    format!(
        "<member>{}{}{}{}{}{}{}</member>",
        el("InstanceId", &i.instance_id),
        el("AvailabilityZone", &i.availability_zone),
        el("LifecycleState", &i.lifecycle_state),
        el("HealthStatus", &i.health_status),
        i.launch_configuration_name
            .as_deref()
            .map(|n| el("LaunchConfigurationName", n))
            .unwrap_or_default(),
        el(
            "ProtectedFromScaleIn",
            &i.protected_from_scale_in.to_string()
        ),
        if with_group {
            el("AutoScalingGroupName", &g.name)
        } else {
            String::new()
        },
    )
}

fn activity_member(a: &ScalingActivity) -> String {
    format!(
        "<member>{}{}{}{}{}{}{}{}</member>",
        el("ActivityId", &a.activity_id),
        el("AutoScalingGroupName", &a.auto_scaling_group_name),
        el("Description", &a.description),
        el("Cause", &a.cause),
        el("StartTime", &iso(a.start_time)),
        a.end_time
            .map(|t| el("EndTime", &iso(t)))
            .unwrap_or_default(),
        el("StatusCode", &a.status_code),
        el("Progress", &a.progress.to_string()),
    )
}

#[async_trait]
impl AwsService for AutoScalingService {
    fn service_name(&self) -> &str {
        "autoscaling"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutating = matches!(
            req.action.as_str(),
            "CreateLaunchConfiguration"
                | "DeleteLaunchConfiguration"
                | "CreateAutoScalingGroup"
                | "UpdateAutoScalingGroup"
                | "DeleteAutoScalingGroup"
                | "SetDesiredCapacity"
                | "CreateOrUpdateTags"
                | "DeleteTags"
        );
        let result = match req.action.as_str() {
            "CreateLaunchConfiguration" => self.create_launch_configuration(&req),
            "DescribeLaunchConfigurations" => self.describe_launch_configurations(&req),
            "DeleteLaunchConfiguration" => self.delete_launch_configuration(&req),
            "CreateAutoScalingGroup" => self.create_auto_scaling_group(&req),
            "DescribeAutoScalingGroups" => self.describe_auto_scaling_groups(&req),
            "UpdateAutoScalingGroup" => self.update_auto_scaling_group(&req),
            "DeleteAutoScalingGroup" => self.delete_auto_scaling_group(&req),
            "SetDesiredCapacity" => self.set_desired_capacity(&req),
            "DescribeAutoScalingInstances" => self.describe_auto_scaling_instances(&req),
            "DescribeScalingActivities" => self.describe_scaling_activities(&req),
            "CreateOrUpdateTags" => self.create_or_update_tags(&req),
            "DeleteTags" => self.delete_tags(&req),
            "DescribeTags" => self.describe_tags(&req),
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!("Action {other} is not supported"),
            )),
        };
        if mutating && result.is_ok() {
            self.save_snapshot().await;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AutoScalingAccounts;
    use std::collections::HashMap;

    fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut qp = HashMap::new();
        for (k, v) in params {
            qp.insert((*k).to_string(), (*v).to_string());
        }
        AwsRequest {
            service: "autoscaling".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "t".into(),
            headers: http::HeaderMap::new(),
            query_params: qp,
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn body(svc: &AutoScalingService, action: &str, params: &[(&str, &str)]) -> String {
        let r = futures_block(svc.handle(req(action, params)));
        String::from_utf8_lossy(r.unwrap().body.expect_bytes()).to_string()
    }

    fn futures_block<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn svc() -> AutoScalingService {
        AutoScalingService::new(Arc::new(parking_lot::RwLock::new(
            AutoScalingAccounts::new(),
        )))
    }

    #[test]
    fn create_asg_reconciles_to_desired_capacity() {
        let s = svc();
        body(
            &s,
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc1"),
                ("ImageId", "ami-1"),
                ("InstanceType", "t3.micro"),
            ],
        );
        body(
            &s,
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "asg1"),
                ("LaunchConfigurationName", "lc1"),
                ("MinSize", "1"),
                ("MaxSize", "5"),
                ("DesiredCapacity", "3"),
                ("AvailabilityZones.member.1", "us-east-1a"),
            ],
        );
        let desc = body(&s, "DescribeAutoScalingGroups", &[]);
        assert_eq!(
            desc.matches("<LifecycleState>InService</LifecycleState>")
                .count(),
            3
        );
        assert!(desc.contains("<DesiredCapacity>3</DesiredCapacity>"));
        // DescribeScalingActivities returns the Successful launch activities the
        // Terraform create waiter blocks on.
        let acts = body(
            &s,
            "DescribeScalingActivities",
            &[("AutoScalingGroupName", "asg1")],
        );
        assert_eq!(
            acts.matches("<StatusCode>Successful</StatusCode>").count(),
            3
        );

        // Scale up then down via SetDesiredCapacity.
        body(
            &s,
            "SetDesiredCapacity",
            &[("AutoScalingGroupName", "asg1"), ("DesiredCapacity", "5")],
        );
        assert_eq!(
            body(&s, "DescribeAutoScalingGroups", &[])
                .matches("<LifecycleState>InService</LifecycleState>")
                .count(),
            5
        );
        body(
            &s,
            "SetDesiredCapacity",
            &[("AutoScalingGroupName", "asg1"), ("DesiredCapacity", "0")],
        );
        assert_eq!(
            body(&s, "DescribeAutoScalingGroups", &[])
                .matches("<LifecycleState>InService</LifecycleState>")
                .count(),
            0
        );
    }

    #[test]
    fn asg_requires_launch_source() {
        let s = svc();
        let err = match futures_block(s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "x"),
                ("MinSize", "1"),
                ("MaxSize", "1"),
            ],
        ))) {
            Err(e) => e,
            Ok(_) => panic!("expected ValidationError"),
        };
        assert_eq!(err.code(), "ValidationError");
    }
}
