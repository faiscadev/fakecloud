//! EC2 Auto Scaling (`autoscaling`) Query-protocol service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::{SnapshotHook, SnapshotStore};

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
    /// EC2 backend so an ASG scales to REAL container-backed instances (the
    /// #8367 wedge) instead of mock ids. `None` falls back to metadata-only
    /// instances (unit tests).
    ec2_state: Option<fakecloud_ec2::SharedEc2State>,
    ec2_runtime: Option<Arc<fakecloud_ec2::Ec2Runtime>>,
}

impl AutoScalingService {
    pub fn new(state: SharedAutoScalingState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            ec2_state: None,
            ec2_runtime: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Attach the EC2 backend so desired-capacity reconciliation launches real
    /// container-backed instances via `RunInstances`.
    pub fn with_ec2(
        mut self,
        state: fakecloud_ec2::SharedEc2State,
        runtime: Option<Arc<fakecloud_ec2::Ec2Runtime>>,
    ) -> Self {
        self.ec2_state = Some(state);
        self.ec2_runtime = runtime;
        self
    }

    /// Launch `count` real EC2 instances for a group, returning their ids.
    /// Falls back to empty (caller synthesizes ids) when no EC2 backend is
    /// wired. Real instances reconcile to `running` via the EC2 runtime (or
    /// metadata-only when there's no container runtime, e.g. CI).
    async fn run_ec2_instances(
        &self,
        image_id: &str,
        instance_type: &str,
        subnet: Option<&str>,
        count: usize,
        req: &AwsRequest,
    ) -> Vec<String> {
        let Some(ec2_state) = self.ec2_state.clone() else {
            return Vec::new();
        };
        let svc =
            fakecloud_ec2::Ec2Service::with_state(ec2_state).with_runtime(self.ec2_runtime.clone());
        let mut params = std::collections::HashMap::new();
        params.insert("ImageId".to_string(), image_id.to_string());
        params.insert("InstanceType".to_string(), instance_type.to_string());
        params.insert("MinCount".to_string(), count.to_string());
        params.insert("MaxCount".to_string(), count.to_string());
        if let Some(s) = subnet {
            params.insert("SubnetId".to_string(), s.to_string());
        }
        let run_req = ec2_request("RunInstances", params, req);
        match svc.handle(run_req).await {
            Ok(resp) => {
                let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
                parse_instance_ids(&body)
            }
            Err(_) => Vec::new(),
        }
    }

    async fn terminate_ec2_instances(&self, ids: &[String], req: &AwsRequest) {
        let Some(ec2_state) = self.ec2_state.clone() else {
            return;
        };
        if ids.is_empty() {
            return;
        }
        let svc =
            fakecloud_ec2::Ec2Service::with_state(ec2_state).with_runtime(self.ec2_runtime.clone());
        let mut params = std::collections::HashMap::new();
        for (n, id) in ids.iter().enumerate() {
            params.insert(format!("InstanceId.{}", n + 1), id.clone());
        }
        let _ = svc
            .handle(ec2_request("TerminateInstances", params, req))
            .await;
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

    /// CloudFormation write-through hook. The CFN provisioner mutates
    /// `autoscaling_state` directly (not through this service's handlers), so
    /// without this hook a CFN-provisioned ASG / launch configuration would
    /// never hit the snapshot and would vanish on restart (#1766 class).
    pub fn snapshot_hook(&self) -> Option<SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let store = store.clone();
            let state = state.clone();
            let lock = lock.clone();
            Box::pin(async move {
                let _guard = lock.lock().await;
                let bytes = {
                    let snap = AutoScalingSnapshot {
                        schema_version: AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
                        accounts: Some(state.read().clone()),
                    };
                    serde_json::to_vec(&snap).unwrap_or_default()
                };
                let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
            })
        }))
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

/// Build an EC2 `AwsRequest` carrying the originating account/region so the
/// launched instances land in the caller's account.
fn ec2_request(
    action: &str,
    params: std::collections::HashMap<String, String>,
    src: &AwsRequest,
) -> AwsRequest {
    AwsRequest {
        service: "ec2".to_string(),
        action: action.to_string(),
        region: src.region.clone(),
        account_id: src.account_id.clone(),
        request_id: src.request_id.clone(),
        headers: http::HeaderMap::new(),
        query_params: params,
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

/// Pull every `<instanceId>…</instanceId>` out of a RunInstances response.
fn parse_instance_ids(xml: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(s) = rest.find("<instanceId>") {
        let after = &rest[s + "<instanceId>".len()..];
        if let Some(e) = after.find("</instanceId>") {
            out.push(after[..e].to_string());
            rest = &after[e + "</instanceId>".len()..];
        } else {
            break;
        }
    }
    out
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
            // InstanceMonitoring.Enabled defaults to true (AWS + Terraform).
            instance_monitoring: optional_query_param(req, "InstanceMonitoring.Enabled")
                .map(|v| v == "true")
                .unwrap_or(true),
            ebs_optimized: optional_query_param(req, "EbsOptimized")
                .map(|v| v == "true")
                .unwrap_or(false),
            spot_price: optional_query_param(req, "SpotPrice"),
            placement_tenancy: optional_query_param(req, "PlacementTenancy"),
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
                let sgs: String = lc
                    .security_groups
                    .iter()
                    .map(|s| format!("<member>{}</member>", xesc(s)))
                    .collect();
                let monitoring = format!(
                    "<InstanceMonitoring><Enabled>{}</Enabled></InstanceMonitoring>",
                    lc.instance_monitoring
                );
                format!(
                    "<member>{}{}{}{}{}{}{monitoring}{}{}{}<SecurityGroups>{sgs}</SecurityGroups>{}{}{}{}</member>",
                    el("LaunchConfigurationName", &lc.name),
                    el("LaunchConfigurationARN", &lc.arn),
                    el("ImageId", &lc.image_id),
                    el("InstanceType", &lc.instance_type),
                    el("KeyName", lc.key_name.as_deref().unwrap_or("")),
                    el("IamInstanceProfile", lc.iam_instance_profile.as_deref().unwrap_or("")),
                    el("EbsOptimized", &lc.ebs_optimized.to_string()),
                    el(
                        "AssociatePublicIpAddress",
                        &lc.associate_public_ip_address.unwrap_or(false).to_string(),
                    ),
                    el("SpotPrice", lc.spot_price.as_deref().unwrap_or("")),
                    el("PlacementTenancy", lc.placement_tenancy.as_deref().unwrap_or("")),
                    lc.user_data
                        .as_deref()
                        .map(|u| el("UserData", u))
                        .unwrap_or_default(),
                    "<BlockDeviceMappings/>",
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

    async fn create_auto_scaling_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
        let group = AutoScalingGroup {
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
            service_linked_role_arn: optional_query_param(req, "ServiceLinkedRoleARN")
                .unwrap_or_else(|| {
                    format!(
                        "arn:aws:iam::{}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
                        req.account_id
                    )
                }),
        };

        let _ = azs;
        {
            let mut accounts = self.state.write();
            accounts
                .get_or_create(&req.account_id)
                .groups
                .insert(name.clone(), group);
        }
        // Reconcile to desired capacity off-lock: launch real container-backed
        // instances so DescribeAutoScalingGroups reports `desired_capacity`
        // InService instances (the Terraform create waiter blocks on this).
        self.apply_capacity(&req.account_id, &name, req).await;
        Ok(self.ok("CreateAutoScalingGroup", String::new(), req))
    }

    async fn update_auto_scaling_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let Some(group) = st.groups.get_mut(&name) else {
                return Err(group_not_found(&name));
            };
            if let Some(v) = optional_query_param(req, "MinSize").and_then(|v| v.parse().ok()) {
                group.min_size = v;
            }
            if let Some(v) = optional_query_param(req, "MaxSize").and_then(|v| v.parse().ok()) {
                group.max_size = v;
            }
            if let Some(v) =
                optional_query_param(req, "DesiredCapacity").and_then(|v| v.parse().ok())
            {
                group.desired_capacity = v;
            }
            if let Some(v) = optional_query_param(req, "LaunchConfigurationName") {
                group.launch_configuration_name = Some(v);
            }
            if let Some(v) = optional_query_param(req, "HealthCheckType") {
                group.health_check_type = v;
            }
        }
        self.apply_capacity(&req.account_id, &name, req).await;
        Ok(self.ok("UpdateAutoScalingGroup", String::new(), req))
    }

    async fn set_desired_capacity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        let desired = required_query_param(req, "DesiredCapacity")?
            .parse::<i64>()
            .unwrap_or(0);
        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let Some(group) = st.groups.get_mut(&name) else {
                return Err(group_not_found(&name));
            };
            group.desired_capacity = desired;
        }
        self.apply_capacity(&req.account_id, &name, req).await;
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
impl AutoScalingService {
    /// Reconcile a group's instance set to its desired capacity. Launches real
    /// container-backed EC2 instances via RunInstances (resolving the image /
    /// type from the group's launch configuration, falling back to a seeded
    /// AMI), or terminates them on scale-in. Records a Successful activity per
    /// change. The EC2 calls happen OFF the state lock (no `.await` under the
    /// parking_lot guard); the lock is only taken to read inputs and apply
    /// results.
    async fn apply_capacity(&self, account: &str, name: &str, req: &AwsRequest) {
        let (target, current_ids, azs, image_id, instance_type, subnet) = {
            let accounts = self.state.read();
            let Some(st) = accounts.accounts.get(account) else {
                return;
            };
            let Some(g) = st.groups.get(name) else {
                return;
            };
            let (image_id, instance_type) = g
                .launch_configuration_name
                .as_ref()
                .and_then(|lc| st.launch_configurations.get(lc))
                .map(|lc| (lc.image_id.clone(), lc.instance_type.clone()))
                .unwrap_or_else(|| {
                    // Launch-template-backed (the LT data isn't stored on the
                    // EC2 side) or unresolved: a seeded public AMI + a common
                    // type still boots a real instance.
                    ("ami-0a1b2c3d4e5f60001".to_string(), "t3.micro".to_string())
                });
            (
                g.desired_capacity.max(0) as usize,
                g.instances
                    .iter()
                    .map(|i| i.instance_id.clone())
                    .collect::<Vec<_>>(),
                g.availability_zones.clone(),
                image_id,
                instance_type,
                g.vpc_zone_identifier
                    .as_ref()
                    .and_then(|v| v.split(',').next().map(|s| s.trim().to_string())),
            )
        };

        let mut launched: Vec<AsgInstance> = Vec::new();
        let mut terminate_ids: Vec<String> = Vec::new();

        if current_ids.len() < target {
            let need = target - current_ids.len();
            let mut ids = self
                .run_ec2_instances(&image_id, &instance_type, subnet.as_deref(), need, req)
                .await;
            // No EC2 backend wired (unit tests) or a partial launch: synthesize
            // the remainder so the group still reports its desired capacity.
            while ids.len() < need {
                ids.push(gen_instance_id());
            }
            for (k, id) in ids.into_iter().enumerate() {
                let az = azs
                    .get(k % azs.len().max(1))
                    .cloned()
                    .unwrap_or_else(|| format!("{}a", req.region));
                launched.push(AsgInstance {
                    instance_id: id,
                    availability_zone: az,
                    lifecycle_state: "InService".to_string(),
                    health_status: "Healthy".to_string(),
                    launch_configuration_name: None,
                    protected_from_scale_in: false,
                });
            }
        } else if current_ids.len() > target {
            let remove = current_ids.len() - target;
            terminate_ids = current_ids.iter().rev().take(remove).cloned().collect();
            self.terminate_ec2_instances(&terminate_ids, req).await;
        }

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(account);
        let descs: Vec<String> = {
            let Some(g) = st.groups.get_mut(name) else {
                return;
            };
            let lcn = g.launch_configuration_name.clone();
            let prot = g.new_instances_protected_from_scale_in;
            let mut descs = Vec::new();
            for mut ni in launched {
                ni.launch_configuration_name = lcn.clone();
                ni.protected_from_scale_in = prot;
                descs.push(format!("Launching a new EC2 instance: {}", ni.instance_id));
                g.instances.push(ni);
            }
            if !terminate_ids.is_empty() {
                g.instances
                    .retain(|i| !terminate_ids.contains(&i.instance_id));
                for id in &terminate_ids {
                    descs.push(format!("Terminating EC2 instance: {id}"));
                }
            }
            descs
        };
        for d in descs {
            st.activities.insert(0, activity(name, &d));
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
         <Tags>{tags}</Tags>{}{}{}</member>",
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
        el("ServiceLinkedRoleARN", &g.service_linked_role_arn),
        "<AvailabilityZoneDistribution><CapacityDistributionStrategy>balanced-best-effort</CapacityDistributionStrategy></AvailabilityZoneDistribution>",
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
            "CreateAutoScalingGroup" => self.create_auto_scaling_group(&req).await,
            "DescribeAutoScalingGroups" => self.describe_auto_scaling_groups(&req),
            "UpdateAutoScalingGroup" => self.update_auto_scaling_group(&req).await,
            "DeleteAutoScalingGroup" => self.delete_auto_scaling_group(&req),
            "SetDesiredCapacity" => self.set_desired_capacity(&req).await,
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
