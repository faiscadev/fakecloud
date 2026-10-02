//! EC2 Auto Scaling (`autoscaling`) Query-protocol service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
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
    /// EC2 whole-state snapshot hook. Reconciling desired capacity launches
    /// (and terminates) REAL EC2 instances through a bare `Ec2Service` built
    /// without a snapshot store, so those EC2 records lived only in memory and
    /// leaked their containers on restart (EC2 boot-recovery had no persisted
    /// `pending`/`running` row to re-drive). Firing this hook after an EC2
    /// mutation writes the EC2 state through, the same way a direct
    /// `RunInstances` API call persists (bug-hunt restart-dataloss).
    ec2_snapshot_hook: Option<SnapshotHook>,
    /// KMS hook the EC2 launches resolve encrypted volumes' keys through
    /// (`aws/ebs` for an encrypted mapping without a key).
    kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    /// Service Quotas, so launched instances are held to the account's
    /// applied security-group quotas.
    quota_provider: Option<Arc<dyn fakecloud_core::quota::QuotaProvider>>,
}

impl AutoScalingService {
    pub fn new(state: SharedAutoScalingState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            ec2_state: None,
            ec2_runtime: None,
            ec2_snapshot_hook: None,
            kms_hook: None,
            quota_provider: None,
        }
    }

    /// Attach the KMS hook so encrypted block-device volumes on launched
    /// instances get the key EC2 would use (the region's `aws/ebs` key when
    /// the mapping names none).
    pub fn with_kms_hook(
        mut self,
        hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    ) -> Self {
        self.kms_hook = hook;
        self
    }

    /// Attach Service Quotas for the security-group quotas EC2 launches are
    /// held to.
    pub fn with_quota_provider(
        mut self,
        provider: Option<Arc<dyn fakecloud_core::quota::QuotaProvider>>,
    ) -> Self {
        self.quota_provider = provider;
        self
    }

    /// A bare EC2 service over the wired EC2 state, for the launches and
    /// terminations this group drives.
    fn ec2_service(&self) -> Option<fakecloud_ec2::Ec2Service> {
        let state = self.ec2_state.clone()?;
        Some(
            fakecloud_ec2::Ec2Service::with_state(state)
                .with_runtime(self.ec2_runtime.clone())
                .with_kms_hook(self.kms_hook.clone())
                .with_quota_provider(self.quota_provider.clone()),
        )
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Attach the EC2 snapshot hook so ASG-launched EC2 instances are persisted
    /// after capacity reconciliation. Without it, ASG-launched instances live
    /// only in memory and their backing containers leak on restart (there is no
    /// persisted EC2 row for boot-recovery to re-drive). `None` (memory mode /
    /// unit tests) makes the persist a no-op.
    pub fn with_ec2_snapshot_hook(mut self, hook: Option<SnapshotHook>) -> Self {
        self.ec2_snapshot_hook = hook;
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

    /// Launch one EC2 instance for a group through `RunInstances` with the
    /// given parameters, returning its id and availability zone, or the
    /// launch error's message.
    async fn run_ec2_instance(
        &self,
        svc: &fakecloud_ec2::Ec2Service,
        mut params: std::collections::HashMap<String, String>,
        req: &AwsRequest,
    ) -> Result<(String, String), String> {
        params.insert("MinCount".to_string(), "1".to_string());
        params.insert("MaxCount".to_string(), "1".to_string());
        let resp = svc
            .handle(ec2_request("RunInstances", params, req))
            .await
            .map_err(|e| e.message())?;
        let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        let id = parse_instance_ids(&body)
            .into_iter()
            .next()
            .ok_or_else(|| "RunInstances returned no instance".to_string())?;
        let az = body
            .split("<availabilityZone>")
            .nth(1)
            .and_then(|r| r.split("</availabilityZone>").next())
            .unwrap_or_default()
            .to_string();
        Ok((id, az))
    }

    /// Terminate the REAL EC2 instances that backed a CFN-provisioned Auto
    /// Scaling Group when its stack is deleted. Public entry for the
    /// CloudFormation delete drain: the group record itself has already been
    /// removed by the synchronous provisioner delete, so this reaps the
    /// orphaned instance containers that the direct `DeleteAutoScalingGroup`
    /// path leaves running. No-op when there is no EC2 backend wired.
    pub async fn cfn_terminate_instances(&self, account_id: &str, region: &str, ids: &[String]) {
        if ids.is_empty() {
            return;
        }
        let req = AwsRequest {
            service: "autoscaling".to_string(),
            action: "DeleteAutoScalingGroup".to_string(),
            region: region.to_string(),
            account_id: account_id.to_string(),
            request_id: Uuid::new_v4().to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        };
        self.terminate_ec2_instances(ids, &req).await;
        self.persist_ec2().await;
    }

    /// Fire the EC2 snapshot hook. The backing instances are driven through a
    /// bare `Ec2Service` with no snapshot store, so a launch or terminate here
    /// is lost on restart unless the hook persists it. No-op when unwired.
    async fn persist_ec2(&self) {
        if let Some(hook) = &self.ec2_snapshot_hook {
            hook().await;
        }
    }

    async fn terminate_ec2_instances(&self, ids: &[String], req: &AwsRequest) {
        let Some(ec2_state) = self.ec2_state.clone() else {
            return;
        };
        if ids.is_empty() {
            return;
        }
        let svc = fakecloud_ec2::Ec2Service::with_state(ec2_state)
            .with_runtime(self.ec2_runtime.clone())
            .with_kms_hook(self.kms_hook.clone())
            .with_quota_provider(self.quota_provider.clone());
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

pub(crate) fn el(name: &str, value: &str) -> String {
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

/// Parse `Filters.member.N.{Name, Values.member.M}` into `(name, values)` pairs.
fn parse_filters(req: &AwsRequest) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for n in 1..=50 {
        let Some(name) = req
            .query_params
            .get(&format!("Filters.member.{n}.Name"))
            .or_else(|| req.query_params.get(&format!("Filters.{n}.Name")))
        else {
            break;
        };
        let mut values = Vec::new();
        for m in 1..=50 {
            let v = req
                .query_params
                .get(&format!("Filters.member.{n}.Values.member.{m}"))
                .or_else(|| req.query_params.get(&format!("Filters.{n}.Values.{m}")));
            match v {
                Some(val) => values.push(val.clone()),
                None => break,
            }
        }
        out.push((name.clone(), values));
    }
    out
}

/// True if the ASG satisfies every tag filter (AWS ANDs filters). Supports the
/// documented `tag:<key>`, `tag-key`, `tag-value`, and `auto-scaling-group`
/// filter names; unknown names are ignored (match-all), matching AWS leniency.
fn group_matches_filters(g: &AutoScalingGroup, filters: &[(String, Vec<String>)]) -> bool {
    filters.iter().all(|(name, values)| {
        let hit = |b: bool| values.is_empty() || b;
        if let Some(key) = name.strip_prefix("tag:") {
            hit(g
                .tags
                .iter()
                .any(|t| t.key == key && values.contains(&t.value)))
        } else {
            match name.as_str() {
                "tag-key" => g.tags.iter().any(|t| values.contains(&t.key)),
                "tag-value" => g.tags.iter().any(|t| values.contains(&t.value)),
                "auto-scaling-group" => hit(values.contains(&g.name)),
                _ => true,
            }
        }
    })
}

/// Offset-based pagination over already-rendered `<member>` strings. AWS uses an
/// opaque NextToken; an integer offset is a faithful-enough opaque token. Returns
/// the page body plus the next offset when the result was truncated.
fn paginate(
    members: Vec<String>,
    req: &AwsRequest,
    default_max: usize,
    max_cap: usize,
) -> (String, Option<usize>) {
    let start = optional_query_param(req, "NextToken")
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0)
        .min(members.len());
    let max = optional_query_param(req, "MaxRecords")
        .and_then(|m| m.parse::<usize>().ok())
        .unwrap_or(default_max)
        .clamp(1, max_cap);
    let end = (start + max).min(members.len());
    let page = members[start..end].concat();
    let next = (end < members.len()).then_some(end);
    (page, next)
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

/// The Auto Scaling service-linked role a group uses when created without a
/// `ServiceLinkedRoleARN`, in `region`'s partition.
pub fn service_linked_role_arn(region: &str, account_id: &str) -> String {
    Arn::global_in(
        region,
        "iam",
        account_id,
        "role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
    )
    .to_string()
}

/// The ARN of an Auto Scaling resource of `kind` (`autoScalingGroup`,
/// `launchConfiguration`), e.g.
/// `arn:aws:autoscaling:us-east-1:123:autoScalingGroup:<id>:autoScalingGroupName/<name>`.
pub fn autoscaling_arn(region: &str, account_id: &str, kind: &str, id: &str, name: &str) -> String {
    Arn::regional(
        "autoscaling",
        region,
        account_id,
        &format!("{kind}:{id}:{kind}Name/{name}"),
    )
    .to_string()
}

impl AutoScalingService {
    fn arn(&self, account: &str, region: &str, kind: &str, name: &str) -> String {
        autoscaling_arn(region, account, kind, &Uuid::new_v4().to_string(), name)
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
            source_instance_id: None,
            block_device_mappings: crate::launch::parse_block_device_mappings(req),
            metadata_options: crate::launch::parse_metadata_options(req),
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
        let members: Vec<String> = st
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
                let block_devices = format!(
                        "{}{}",
                        crate::launch::block_device_mappings_xml(&lc.block_device_mappings),
                        lc.metadata_options
                            .as_ref()
                            .map(|m| format!(
                                "<MetadataOptions>{}{}{}</MetadataOptions>",
                                m.http_tokens
                                    .as_deref()
                                    .map(|v| el("HttpTokens", v))
                                    .unwrap_or_default(),
                                m.http_put_response_hop_limit
                                    .map(|v| el("HttpPutResponseHopLimit", &v.to_string()))
                                    .unwrap_or_default(),
                                m.http_endpoint
                                    .as_deref()
                                    .map(|v| el("HttpEndpoint", v))
                                    .unwrap_or_default(),
                            ))
                            .unwrap_or_default(),
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
                    block_devices,
                    el("CreatedTime", &iso(lc.created_time)),
                )
            })
            .collect();
        let (items, next) = paginate(members, req, 100, 100);
        let token = next
            .map(|n| el("NextToken", &n.to_string()))
            .unwrap_or_default();
        let inner = format!("<LaunchConfigurations>{items}</LaunchConfigurations>{token}");
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

        let mut launch_template = parse_launch_template(req);
        let mut mixed_instances_policy = crate::launch::parse_mixed_instances_policy(req);
        let mut launch_configuration_name = optional_query_param(req, "LaunchConfigurationName");
        let instance_id = optional_query_param(req, "InstanceId");
        let sources = [
            launch_configuration_name.is_some(),
            launch_template.is_some(),
            mixed_instances_policy.is_some(),
            instance_id.is_some(),
        ]
        .iter()
        .filter(|s| **s)
        .count();
        // Exactly one launch source.
        if sources != 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "Valid requests must contain either LaunchTemplate, LaunchConfigurationName, \
                 InstanceId or MixedInstancesPolicy parameter.",
            ));
        }
        self.validate_launch_source(
            req,
            launch_configuration_name.as_deref(),
            launch_template.as_mut(),
            mixed_instances_policy.as_mut(),
        )?;

        let mut azs = member_list(req, "AvailabilityZones");
        let mut vpc_zone_identifier = optional_query_param(req, "VPCZoneIdentifier");
        // `InstanceId`: AWS derives a launch configuration named after the
        // group from the instance, and launches into its subnet / AZ unless
        // the request names others.
        let mut derived_lc = None;
        if let Some(iid) = &instance_id {
            let Some(ec2_state) = &self.ec2_state else {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Invalid instance id {iid}"),
                ));
            };
            let (lc, subnet, az) = crate::launch::launch_configuration_from_instance(
                ec2_state,
                &req.account_id,
                &req.region,
                iid,
                &name,
            )
            .map_err(|m| {
                AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", m)
            })?;
            if vpc_zone_identifier.is_none() && azs.is_empty() {
                match subnet {
                    Some(s) => vpc_zone_identifier = Some(s),
                    None => azs.push(az),
                }
            }
            launch_configuration_name = Some(lc.name.clone());
            derived_lc = Some(lc);
        }
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
                .unwrap_or_else(|| service_linked_role_arn(&req.region, &req.account_id)),
            mixed_instances_policy,
        };

        let _ = azs;
        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            // AWS rejects a duplicate-name create. Without this guard two
            // concurrent same-name creates would each launch desired_capacity
            // real instances and the second insert would orphan the first's.
            if st.groups.contains_key(&name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AlreadyExists",
                    format!("AutoScalingGroup by this name already exists - A group with the name {name} already exists"),
                ));
            }
            if let Some(lc) = derived_lc {
                // The derived configuration takes the group's name; never
                // replace one another group may launch from.
                if st.launch_configurations.contains_key(&lc.name) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExists",
                        format!(
                            "Launch Configuration by this name already exists - A launch configuration already exists with the name {}",
                            lc.name
                        ),
                    ));
                }
                st.launch_configurations.insert(lc.name.clone(), lc);
            }
            st.groups.insert(name.clone(), group);
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
        let new_lc = optional_query_param(req, "LaunchConfigurationName");
        let mut new_lt = parse_launch_template(req);
        let mut new_mixed = crate::launch::parse_mixed_instances_policy(req);
        let sources = [new_lc.is_some(), new_lt.is_some(), new_mixed.is_some()]
            .iter()
            .filter(|s| **s)
            .count();
        if sources > 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "Valid requests must contain either LaunchTemplate, LaunchConfigurationName \
                 or MixedInstancesPolicy parameter.",
            ));
        }
        if sources == 1 {
            self.validate_launch_source(
                req,
                new_lc.as_deref(),
                new_lt.as_mut(),
                new_mixed.as_mut(),
            )?;
        }
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
            // Switching the launch source replaces the previous one.
            if let Some(v) = new_lc {
                group.launch_configuration_name = Some(v);
                group.launch_template = None;
                group.mixed_instances_policy = None;
            }
            if let Some(lt) = new_lt {
                group.launch_template = Some(lt);
                group.launch_configuration_name = None;
                group.mixed_instances_policy = None;
            }
            if let Some(policy) = new_mixed {
                group.mixed_instances_policy = Some(policy);
                group.launch_configuration_name = None;
                group.launch_template = None;
            }
            if let Some(v) = optional_query_param(req, "HealthCheckType") {
                group.health_check_type = v;
            }
            // Terraform's aws_autoscaling_group update sends any of these on
            // HasChange; persisting only Min/Max/Desired/LC/HealthCheckType left
            // the rest as read-after-write drift -> perpetual diff. Apply every
            // mutable field DescribeAutoScalingGroups echoes back.
            if let Some(v) =
                optional_query_param(req, "DefaultCooldown").and_then(|v| v.parse().ok())
            {
                group.default_cooldown = v;
            }
            if let Some(v) =
                optional_query_param(req, "HealthCheckGracePeriod").and_then(|v| v.parse().ok())
            {
                group.health_check_grace_period = v;
            }
            if let Some(v) = optional_query_param(req, "VPCZoneIdentifier") {
                group.vpc_zone_identifier = Some(v);
            }
            let azs = member_list(req, "AvailabilityZones");
            if !azs.is_empty() {
                group.availability_zones = azs;
            }
            if let Some(v) = optional_query_param(req, "NewInstancesProtectedFromScaleIn") {
                group.new_instances_protected_from_scale_in = v == "true";
            }
            if let Some(v) = optional_query_param(req, "ServiceLinkedRoleARN") {
                group.service_linked_role_arn = v;
            }
            let tgs = member_list(req, "TargetGroupARNs");
            if !tgs.is_empty() {
                group.target_group_arns = tgs;
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
            // AWS rejects a desired capacity outside [MinSize, MaxSize].
            if desired < group.min_size || desired > group.max_size {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "New SetDesiredCapacity value {desired} is not in between the MinSize {} and the MaxSize {} of the AutoScalingGroup.",
                        group.min_size, group.max_size
                    ),
                ));
            }
            group.desired_capacity = desired;
        }
        self.apply_capacity(&req.account_id, &name, req).await;
        Ok(self.ok("SetDesiredCapacity", String::new(), req))
    }

    async fn delete_auto_scaling_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "AutoScalingGroupName")?;
        let force = optional_query_param(req, "ForceDelete")
            .map(|v| v == "true")
            .unwrap_or(false);
        // Collect the backing instance ids under the lock, then drop the group.
        // ForceDelete must terminate them; without force a non-empty group is an
        // error. The EC2 termination happens off the lock below.
        let backing_ids = {
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
            st.groups
                .remove(&name)
                .map(|g| {
                    g.instances
                        .into_iter()
                        .map(|i| i.instance_id)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        // ForceDelete reaps the backing EC2 instances instead of leaking them.
        self.terminate_ec2_instances(&backing_ids, req).await;
        if self.ec2_state.is_some() && !backing_ids.is_empty() {
            self.persist_ec2().await;
        }
        Ok(self.ok("DeleteAutoScalingGroup", String::new(), req))
    }

    fn describe_auto_scaling_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let wanted = member_list(req, "AutoScalingGroupNames");
        let filters = parse_filters(req);
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        let members: Vec<String> = st
            .groups
            .values()
            .filter(|g| wanted.is_empty() || wanted.contains(&g.name))
            .filter(|g| group_matches_filters(g, &filters))
            .map(group_xml)
            .collect();
        let (items, next) = paginate(members, req, 100, 100);
        let token = next
            .map(|n| el("NextToken", &n.to_string()))
            .unwrap_or_default();
        let inner = format!("<AutoScalingGroups>{items}</AutoScalingGroups>{token}");
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
        let members: Vec<String> = st
            .groups
            .values()
            .flat_map(|g| g.instances.iter().map(move |i| (g, i)))
            .filter(|(_, i)| wanted.is_empty() || wanted.contains(&i.instance_id))
            .map(|(g, i)| asg_instance_member(g, i, true))
            .collect();
        let (items, next) = paginate(members, req, 50, 50);
        let token = next
            .map(|n| el("NextToken", &n.to_string()))
            .unwrap_or_default();
        let inner = format!("<AutoScalingInstances>{items}</AutoScalingInstances>{token}");
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
        let members: Vec<String> = st
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
        let (items, next) = paginate(members, req, 100, 100);
        let token = next
            .map(|n| el("NextToken", &n.to_string()))
            .unwrap_or_default();
        let inner = format!("<Activities>{items}</Activities>{token}");
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
        let filters = parse_filters(req);
        let accounts = self.state.read();
        let empty = AccountState::default();
        let st = accounts.accounts.get(&req.account_id).unwrap_or(&empty);
        // AWS scopes DescribeTags by the documented Filters (auto-scaling-group,
        // key, value, propagate-at-launch). Returning every group's tags caused
        // tag bleed across resources for aws_autoscaling_group_tag.
        let tag_ok = |group_name: &str, t: &AsgTag| {
            filters.iter().all(|(name, values)| {
                if values.is_empty() {
                    return true;
                }
                match name.as_str() {
                    "auto-scaling-group" => values.iter().any(|v| v == group_name),
                    "key" => values.contains(&t.key),
                    "value" => values.contains(&t.value),
                    "propagate-at-launch" => values
                        .iter()
                        .any(|v| v == &t.propagate_at_launch.to_string()),
                    _ => true,
                }
            })
        };
        let members: Vec<String> = st
            .groups
            .values()
            .flat_map(|g| g.tags.iter().map(move |t| (g, t)))
            .filter(|(g, t)| tag_ok(&g.name, t))
            .map(|(g, t)| {
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
        let (items, next) = paginate(members, req, 100, 100);
        let token = next
            .map(|n| el("NextToken", &n.to_string()))
            .unwrap_or_default();
        let inner = format!("<Tags>{items}</Tags>{token}");
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

/// What one reconcile launches from: one plan, or a mixed-instances
/// policy's plan per override.
enum LaunchPlans {
    Single(LaunchPlan),
    Mixed {
        policy: Box<crate::state::MixedInstancesPolicy>,
        plans: Vec<LaunchPlan>,
    },
}

/// How one reconcile launches each new instance of a group.
struct LaunchPlan {
    /// The `RunInstances` parameters (before placement and tags).
    params: std::collections::HashMap<String, String>,
    /// What the group records about each launched instance.
    instance_type: Option<String>,
    launch_template: Option<LaunchTemplateSpec>,
    weighted_capacity: Option<String>,
    /// Whether the source itself launches Spot (a launch configuration's
    /// `SpotPrice`, or a launch template's `InstanceMarketOptions`).
    spot: bool,
}

/// The capacity units an instance counts for (its mixed-instances weight,
/// else 1).
fn weight_of(w: &Option<String>) -> i64 {
    w.as_deref()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1)
}

/// The instances (oldest first in `instances`) a scale-in to `target`
/// terminates: repeatedly the heaviest one (newest first among equals) whose
/// removal still leaves the target covered, so a weighted group gets as close
/// to its desired capacity as it can.
fn scale_in_choice(mut instances: Vec<(String, i64)>, target: i64) -> Vec<String> {
    let mut cap: i64 = instances.iter().map(|(_, w)| w).sum();
    let mut out = Vec::new();
    while let Some(pos) = instances
        .iter()
        .enumerate()
        .filter(|(_, (_, w))| cap - w >= target)
        .max_by_key(|(i, (_, w))| (*w, *i))
        .map(|(i, _)| i)
    {
        let (id, w) = instances.remove(pos);
        cap -= w;
        out.push(id);
    }
    out
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
    /// Reconcile a group to its desired capacity outside the request path (e.g.
    /// CloudFormation provisioning). Synthesizes a minimal `AwsRequest` — the
    /// EC2 calls only read `region`/`account_id`/`request_id` off it — and runs
    /// the same `apply_capacity` reconciliation the direct API path uses, so a
    /// CFN-provisioned ASG ends up with REAL container-backed instances (or
    /// synthesized ids when no EC2 backend is wired, e.g. CI).
    pub async fn reconcile_group(&self, account: &str, name: &str, region: &str) {
        let req = AwsRequest {
            service: "autoscaling".to_string(),
            action: "ReconcileGroup".to_string(),
            region: region.to_string(),
            account_id: account.to_string(),
            request_id: "cfn".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        };
        self.apply_capacity(account, name, &req).await;
    }

    /// Check a group's launch source the way CreateAutoScalingGroup /
    /// UpdateAutoScalingGroup do: a named launch configuration must exist, and
    /// a launch template (direct, mixed-instances base, or an override's) must
    /// resolve to an existing version. Resolved templates are recorded with
    /// both their id and name, and `$Default` when no version was given, as
    /// DescribeAutoScalingGroups reports them.
    fn validate_launch_source(
        &self,
        req: &AwsRequest,
        launch_configuration: Option<&str>,
        launch_template: Option<&mut LaunchTemplateSpec>,
        mixed: Option<&mut crate::state::MixedInstancesPolicy>,
    ) -> Result<(), AwsServiceError> {
        if let Some(lc) = launch_configuration {
            let exists = self
                .state
                .read()
                .accounts
                .get(&req.account_id)
                .is_some_and(|st| st.launch_configurations.contains_key(lc));
            if !exists {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Launch configuration name not found - Launch configuration {lc} not found"
                    ),
                ));
            }
        }
        crate::launch::resolve_launch_template_specs(
            self.ec2_state.as_ref(),
            &req.account_id,
            &req.region,
            launch_template,
            mixed,
        )
        .map_err(|msg| AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", msg))
    }

    /// Resolve a group's launch source into the `RunInstances` parameters
    /// each new instance launches with. A launch template is resolved once
    /// here, so every instance of this reconcile launches from (and records)
    /// the same concrete version; `Err` is the reason the launch fails.
    fn launch_plans(
        &self,
        source: &crate::launch::LaunchSource,
        account: &str,
        region: &str,
    ) -> Result<LaunchPlans, String> {
        use crate::launch::LaunchSource;
        let LaunchSource::Mixed(policy) = source else {
            return self
                .launch_plan(source, account, region)
                .map(LaunchPlans::Single);
        };
        // One plan per override (the base template when there are none),
        // each resolved once for the whole reconcile.
        let overrides: Vec<crate::state::LaunchTemplateOverride> = if policy.overrides.is_empty() {
            vec![crate::state::LaunchTemplateOverride::default()]
        } else {
            policy.overrides.clone()
        };
        let plans = overrides
            .iter()
            .map(|o| {
                self.launch_plan(
                    &LaunchSource::Template {
                        spec: o
                            .launch_template_specification
                            .clone()
                            .unwrap_or_else(|| policy.launch_template.clone()),
                        instance_type: o.instance_type.clone(),
                        weighted_capacity: o.weighted_capacity.clone(),
                    },
                    account,
                    region,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LaunchPlans::Mixed {
            policy: policy.clone(),
            plans,
        })
    }

    fn launch_plan(
        &self,
        source: &crate::launch::LaunchSource,
        account: &str,
        region: &str,
    ) -> Result<LaunchPlan, String> {
        use crate::launch::LaunchSource;
        match source {
            LaunchSource::Mixed(_) => {
                Err("a mixed-instances policy has one plan per override".into())
            }
            LaunchSource::Configuration(lc) => Ok(LaunchPlan {
                params: crate::launch::launch_configuration_params(lc),
                instance_type: Some(lc.instance_type.clone()),
                launch_template: None,
                weighted_capacity: None,
                spot: lc.spot_price.as_ref().is_some_and(|p| !p.is_empty()),
            }),
            LaunchSource::MissingConfiguration(name) => {
                Err(format!("Launch configuration {name} not found."))
            }
            LaunchSource::Template {
                spec,
                instance_type,
                weighted_capacity,
            } => {
                let mut params = std::collections::HashMap::new();
                if let Some(t) = instance_type {
                    params.insert("InstanceType".to_string(), t.clone());
                }
                let Some(ec2_state) = &self.ec2_state else {
                    return Ok(LaunchPlan {
                        params,
                        instance_type: instance_type.clone(),
                        launch_template: Some(spec.clone()),
                        weighted_capacity: weighted_capacity.clone(),
                        spot: false,
                    });
                };
                let resolved = fakecloud_ec2::service::launch_template::resolve_launch_template_in(
                    ec2_state,
                    account,
                    region,
                    spec.launch_template_id.as_deref(),
                    // Once resolved, a spec carries both; the id is authoritative.
                    spec.launch_template_name
                        .as_deref()
                        .filter(|_| spec.launch_template_id.is_none()),
                    spec.version.as_deref(),
                )
                .map_err(|e| e.message())?;
                params.insert(
                    "LaunchTemplate.LaunchTemplateId".to_string(),
                    resolved.id.clone(),
                );
                params.insert(
                    "LaunchTemplate.Version".to_string(),
                    resolved.version.to_string(),
                );
                let spot = resolved
                    .data
                    .get("InstanceMarketOptions.MarketType")
                    .is_some_and(|m| m == "spot");
                Ok(LaunchPlan {
                    spot,
                    instance_type: instance_type
                        .clone()
                        .or_else(|| resolved.data.get("InstanceType").cloned()),
                    params,
                    launch_template: Some(LaunchTemplateSpec {
                        launch_template_id: Some(resolved.id),
                        launch_template_name: Some(resolved.name),
                        version: Some(resolved.version.to_string()),
                    }),
                    weighted_capacity: weighted_capacity.clone(),
                })
            }
            LaunchSource::Default => {
                // No launch source recorded: a seeded public AMI + a common
                // type still boots a real instance.
                let mut params = std::collections::HashMap::new();
                params.insert("ImageId".to_string(), "ami-0a1b2c3d4e5f60001".to_string());
                params.insert("InstanceType".to_string(), "t3.micro".to_string());
                Ok(LaunchPlan {
                    params,
                    instance_type: Some("t3.micro".to_string()),
                    launch_template: None,
                    weighted_capacity: None,
                    spot: false,
                })
            }
        }
    }

    async fn apply_capacity(&self, account: &str, name: &str, req: &AwsRequest) {
        let (target, current, azs, subnets, source, instance_tags) = {
            let accounts = self.state.read();
            let Some(st) = accounts.accounts.get(account) else {
                return;
            };
            let Some(g) = st.groups.get(name) else {
                return;
            };
            // Every instance carries the group-name system tag plus the
            // group's propagate-at-launch tags; these win over same-key tags
            // from a launch template (AWS gives the group's value precedence).
            let mut instance_tags = vec![("aws:autoscaling:groupName".to_string(), g.name.clone())];
            for t in g.tags.iter().filter(|t| t.propagate_at_launch) {
                instance_tags.push((t.key.clone(), t.value.clone()));
            }
            (
                g.desired_capacity.max(0),
                g.instances
                    .iter()
                    .map(|i| {
                        (
                            i.instance_id.clone(),
                            i.weighted_capacity.clone(),
                            i.instance_type.clone(),
                            i.lifecycle.is_some(),
                        )
                    })
                    .collect::<Vec<_>>(),
                g.availability_zones.clone(),
                g.vpc_zone_identifier
                    .as_deref()
                    .map(|v| {
                        v.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                crate::launch::LaunchSource::for_group(g, &st.launch_configurations),
                instance_tags,
            )
        };
        let capacity: i64 = current.iter().map(|(_, w, _, _)| weight_of(w)).sum();

        let mut launched: Vec<AsgInstance> = Vec::new();
        let mut failures: Vec<String> = Vec::new();
        let mut terminate_ids: Vec<String> = Vec::new();
        // Whether this reconcile mutated REAL EC2 state (launched or terminated
        // container-backed instances), so it must be persisted through the EC2
        // snapshot hook below. No-op when there is no EC2 backend (the ids are
        // synthesized metadata) or no hook wired.
        let mut ec2_touched = false;

        if capacity < target {
            match self.launch_plans(&source, account, &req.region) {
                Err(reason) => failures.push(reason),
                Ok(plans) => {
                    let ec2 = self.ec2_service();
                    // What the group already runs, for a mixed-instances
                    // policy's on-demand / Spot split and Spot spreading.
                    let mut tally: Vec<crate::launch::MixedTally> = match &plans {
                        LaunchPlans::Single(_) => Vec::new(),
                        LaunchPlans::Mixed { plans, .. } => current
                            .iter()
                            .map(|(_, w, itype, spot)| crate::launch::MixedTally {
                                override_index: plans.iter().position(|p| {
                                    p.instance_type.is_some() && &p.instance_type == itype
                                }),
                                spot: *spot,
                                weight: weight_of(w),
                            })
                            .collect(),
                    };
                    let mut cap = capacity;
                    let mut slot = current.len();
                    while cap < target {
                        let (plan, spot) = match &plans {
                            LaunchPlans::Single(plan) => (plan, false),
                            LaunchPlans::Mixed { policy, plans } => {
                                let weights: Vec<i64> = plans
                                    .iter()
                                    .map(|p| weight_of(&p.weighted_capacity))
                                    .collect();
                                let (i, spot) =
                                    crate::launch::choose_mixed_launch(policy, &tally, &weights);
                                tally.push(crate::launch::MixedTally {
                                    override_index: Some(i),
                                    spot,
                                    weight: weights[i],
                                });
                                (&plans[i], spot)
                            }
                        };
                        let weight = weight_of(&plan.weighted_capacity);
                        let spot_max_price = match &plans {
                            LaunchPlans::Mixed { policy, .. } => policy
                                .instances_distribution
                                .as_ref()
                                .and_then(|d| d.spot_max_price.clone()),
                            LaunchPlans::Single(_) => None,
                        };
                        let is_spot = spot || plan.spot;
                        // Spread across the group's subnets (or, without a
                        // VPC zone identifier, its availability zones).
                        let az_hint = azs
                            .get(slot % azs.len().max(1))
                            .cloned()
                            .unwrap_or_else(|| format!("{}a", req.region));
                        let placed = match &ec2 {
                            // No EC2 backend wired (unit tests): synthesize the
                            // instance so the group still reports its capacity.
                            None => Ok((gen_instance_id(), az_hint.clone())),
                            Some(svc) => {
                                let mut params = plan.params.clone();
                                if spot {
                                    params.insert(
                                        "InstanceMarketOptions.MarketType".to_string(),
                                        "spot".to_string(),
                                    );
                                    if let Some(price) = &spot_max_price {
                                        params.insert(
                                            "InstanceMarketOptions.SpotOptions.MaxPrice"
                                                .to_string(),
                                            price.clone(),
                                        );
                                    }
                                }
                                match subnets.get(slot % subnets.len().max(1)) {
                                    Some(subnet) => {
                                        params.insert("SubnetId".to_string(), subnet.clone());
                                    }
                                    None => {
                                        params.insert(
                                            "Placement.AvailabilityZone".to_string(),
                                            az_hint.clone(),
                                        );
                                    }
                                }
                                params.insert(
                                    "TagSpecification.1.ResourceType".to_string(),
                                    "instance".to_string(),
                                );
                                for (i, (k, v)) in instance_tags.iter().enumerate() {
                                    let n = i + 1;
                                    params.insert(
                                        format!("TagSpecification.1.Tag.{n}.Key"),
                                        k.clone(),
                                    );
                                    params.insert(
                                        format!("TagSpecification.1.Tag.{n}.Value"),
                                        v.clone(),
                                    );
                                }
                                ec2_touched = true;
                                self.run_ec2_instance(svc, params, req).await
                            }
                        };
                        match placed {
                            Ok((id, az)) => launched.push(AsgInstance {
                                instance_id: id,
                                availability_zone: if az.is_empty() { az_hint } else { az },
                                lifecycle_state: "InService".to_string(),
                                health_status: "Healthy".to_string(),
                                launch_configuration_name: None,
                                protected_from_scale_in: false,
                                instance_type: plan.instance_type.clone(),
                                launch_template: plan.launch_template.clone(),
                                weighted_capacity: plan.weighted_capacity.clone(),
                                lifecycle: is_spot.then(|| "spot".to_string()),
                            }),
                            Err(reason) => {
                                failures.push(reason);
                                break;
                            }
                        }
                        cap += weight;
                        slot += 1;
                    }
                }
            }
        } else if capacity > target {
            // Scale in newest-first, keeping the group at or above its
            // desired capacity (a weighted instance is only removed while the
            // rest still covers the target).
            terminate_ids = scale_in_choice(
                current
                    .iter()
                    .map(|(id, w, _, _)| (id.clone(), weight_of(w)))
                    .collect(),
                target,
            );
            self.terminate_ec2_instances(&terminate_ids, req).await;
            ec2_touched = self.ec2_state.is_some() && !terminate_ids.is_empty();
        }

        // Persist the REAL EC2 records this reconcile launched/terminated BEFORE
        // touching the ASG state below. The ASG-launched instances are driven
        // through a bare `Ec2Service` built without a snapshot store, so without
        // firing the EC2 snapshot hook here they live only in memory and leak
        // their containers on restart (EC2 boot-recovery has no persisted row to
        // re-drive). Done before the ASG write so a concurrent group delete (the
        // early `return` below) cannot skip persisting them. No-op when there is
        // no EC2 backend or no hook wired (bug-hunt restart-dataloss).
        if ec2_touched {
            self.persist_ec2().await;
        }

        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(account);
            let mut activities: Vec<ScalingActivity> = Vec::new();
            {
                let Some(g) = st.groups.get_mut(name) else {
                    return;
                };
                let lcn = g.launch_configuration_name.clone();
                let prot = g.new_instances_protected_from_scale_in;
                for mut ni in launched {
                    ni.launch_configuration_name = lcn.clone();
                    ni.protected_from_scale_in = prot;
                    activities.push(activity(
                        name,
                        &format!("Launching a new EC2 instance: {}", ni.instance_id),
                    ));
                    g.instances.push(ni);
                }
                if !terminate_ids.is_empty() {
                    g.instances
                        .retain(|i| !terminate_ids.contains(&i.instance_id));
                    for id in &terminate_ids {
                        activities.push(activity(name, &format!("Terminating EC2 instance: {id}")));
                    }
                }
                for reason in &failures {
                    activities.push(failed_launch_activity(name, reason));
                }
            }
            for a in activities {
                st.activities.insert(0, a);
            }
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
        status_message: None,
    }
}

/// A launch that failed, as AWS records it.
fn failed_launch_activity(group: &str, reason: &str) -> ScalingActivity {
    let message = format!("{reason} Launching EC2 instance failed.");
    let mut a = activity(
        group,
        &format!("Launching a new EC2 instance.  Status Reason: {message}"),
    );
    a.status_code = "Failed".to_string();
    a.status_message = Some(message);
    a
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
    let lbs: String = g
        .load_balancer_names
        .iter()
        .map(|a| format!("<member>{}</member>", xesc(a)))
        .collect();
    // A launch-template-backed ASG (the modern default) must report its
    // LaunchTemplate, or terraform reads it back empty and shows perpetual drift.
    let lt = g
        .launch_template
        .as_ref()
        .map(|lt| {
            format!(
                "<LaunchTemplate>{}</LaunchTemplate>",
                crate::launch::launch_template_spec_xml(lt)
            )
        })
        .unwrap_or_default();
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
    let mixed = g
        .mixed_instances_policy
        .as_ref()
        .map(crate::launch::mixed_instances_policy_xml)
        .unwrap_or_default();
    format!(
        "<member>{}{}{}{lt}{}{}{}{}{}{}{}{}<AvailabilityZones>{azs}</AvailabilityZones>\
         <Instances>{instances}</Instances><TargetGroupARNs>{tgs}</TargetGroupARNs>\
         <LoadBalancerNames>{lbs}</LoadBalancerNames>\
         <Tags>{tags}</Tags>{}{}{}<AvailabilityZoneDistribution><CapacityDistributionStrategy>balanced-best-effort</CapacityDistributionStrategy></AvailabilityZoneDistribution></member>",
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
        mixed,
    )
}

fn asg_instance_member(g: &AutoScalingGroup, i: &AsgInstance, with_group: bool) -> String {
    format!(
        "<member>{}{}{}{}{}{}{}{}{}{}</member>",
        el("InstanceId", &i.instance_id),
        i.instance_type
            .as_deref()
            .map(|t| el("InstanceType", t))
            .unwrap_or_default(),
        i.launch_template
            .as_ref()
            .map(|lt| format!(
                "<LaunchTemplate>{}</LaunchTemplate>",
                crate::launch::launch_template_spec_xml(lt)
            ))
            .unwrap_or_default(),
        i.weighted_capacity
            .as_deref()
            .map(|w| el("WeightedCapacity", w))
            .unwrap_or_default(),
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
        "<member>{}{}{}{}{}{}{}{}{}</member>",
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
        a.status_message
            .as_deref()
            .map(|m| el("StatusMessage", m))
            .unwrap_or_default(),
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
            "DeleteAutoScalingGroup" => self.delete_auto_scaling_group(&req).await,
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
        // Scaling to 0 requires lowering MinSize to 0 first (AWS rejects a
        // desired capacity below MinSize).
        body(
            &s,
            "UpdateAutoScalingGroup",
            &[("AutoScalingGroupName", "asg1"), ("MinSize", "0")],
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

        // And a desired capacity outside [MinSize, MaxSize] is rejected.
        match futures_block(s.handle(req(
            "SetDesiredCapacity",
            &[("AutoScalingGroupName", "asg1"), ("DesiredCapacity", "99")],
        ))) {
            Err(e) => assert!(format!("{e:?}").contains("ValidationError")),
            Ok(_) => panic!("desired capacity above MaxSize must be rejected"),
        }
    }

    #[tokio::test]
    async fn force_delete_terminates_backing_ec2_instances() {
        let account = "123456789012";
        let ec2_state: fakecloud_ec2::SharedEc2State = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(account, "us-east-1", ""),
        ));
        let s = AutoScalingService::new(Arc::new(parking_lot::RwLock::new(
            AutoScalingAccounts::new(),
        )))
        .with_ec2(ec2_state.clone(), None);

        s.handle(req(
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc1"),
                ("ImageId", "ami-1"),
                ("InstanceType", "t3.micro"),
            ],
        ))
        .await
        .unwrap();
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "asg1"),
                ("LaunchConfigurationName", "lc1"),
                ("MinSize", "2"),
                ("MaxSize", "5"),
                ("DesiredCapacity", "2"),
                ("AvailabilityZones.member.1", "us-east-1a"),
            ],
        ))
        .await
        .unwrap();

        // Two real EC2 instances were launched and are running.
        let running: Vec<String> = ec2_state
            .read()
            .default_ref()
            .instances
            .iter()
            .filter(|(_, i)| i.state_name != "terminated")
            .map(|(id, _)| id.clone())
            .collect();
        assert_eq!(running.len(), 2, "ASG must launch 2 real EC2 instances");

        // Force-delete the non-empty group -> the backing instances terminate.
        s.handle(req(
            "DeleteAutoScalingGroup",
            &[("AutoScalingGroupName", "asg1"), ("ForceDelete", "true")],
        ))
        .await
        .unwrap();
        let ec2 = ec2_state.read();
        for id in &running {
            assert_eq!(
                ec2.default_ref().instances.get(id).unwrap().state_name,
                "terminated",
                "instance {id} must be terminated by ForceDelete"
            );
        }
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

    fn create_basic_asg(s: &AutoScalingService, name: &str) {
        body(
            s,
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc"),
                ("ImageId", "ami-1"),
                ("InstanceType", "t3.micro"),
            ],
        );
        body(
            s,
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", name),
                ("LaunchConfigurationName", "lc"),
                ("MinSize", "0"),
                ("MaxSize", "5"),
                ("DesiredCapacity", "0"),
                ("AvailabilityZones.member.1", "us-east-1a"),
            ],
        );
    }

    #[test]
    fn update_persists_full_mutable_field_set() {
        let s = svc();
        create_basic_asg(&s, "asg1");
        body(
            &s,
            "UpdateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "asg1"),
                ("DefaultCooldown", "120"),
                ("HealthCheckGracePeriod", "240"),
                ("VPCZoneIdentifier", "subnet-abc,subnet-def"),
                ("NewInstancesProtectedFromScaleIn", "true"),
                ("HealthCheckType", "ELB"),
            ],
        );
        let d = body(&s, "DescribeAutoScalingGroups", &[]);
        assert!(d.contains("<DefaultCooldown>120</DefaultCooldown>"), "{d}");
        assert!(d.contains("<HealthCheckGracePeriod>240</HealthCheckGracePeriod>"));
        assert!(d.contains("<VPCZoneIdentifier>subnet-abc,subnet-def</VPCZoneIdentifier>"));
        assert!(
            d.contains("<NewInstancesProtectedFromScaleIn>true</NewInstancesProtectedFromScaleIn>")
        );
        assert!(d.contains("<HealthCheckType>ELB</HealthCheckType>"));
    }

    #[test]
    fn duplicate_create_rejected() {
        let s = svc();
        create_basic_asg(&s, "dup");
        let err = match futures_block(s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "dup"),
                ("LaunchConfigurationName", "lc"),
                ("MinSize", "0"),
                ("MaxSize", "1"),
            ],
        ))) {
            Err(e) => e,
            Ok(_) => panic!("expected AlreadyExists"),
        };
        assert_eq!(err.code(), "AlreadyExists");
    }

    #[test]
    fn describe_groups_filters_by_tag() {
        let s = svc();
        create_basic_asg(&s, "asg1");
        body(
            &s,
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "asg2"),
                ("LaunchConfigurationName", "lc"),
                ("MinSize", "0"),
                ("MaxSize", "1"),
                ("AvailabilityZones.member.1", "us-east-1a"),
            ],
        );
        body(
            &s,
            "CreateOrUpdateTags",
            &[
                ("Tags.member.1.ResourceId", "asg1"),
                ("Tags.member.1.Key", "Env"),
                ("Tags.member.1.Value", "prod"),
                ("Tags.member.1.PropagateAtLaunch", "true"),
            ],
        );
        let d = body(
            &s,
            "DescribeAutoScalingGroups",
            &[
                ("Filters.member.1.Name", "tag:Env"),
                ("Filters.member.1.Values.member.1", "prod"),
            ],
        );
        assert!(d.contains("<AutoScalingGroupName>asg1</AutoScalingGroupName>"));
        assert!(
            !d.contains("<AutoScalingGroupName>asg2</AutoScalingGroupName>"),
            "{d}"
        );
    }

    #[test]
    fn describe_tags_scoped_by_filter() {
        let s = svc();
        create_basic_asg(&s, "asg1");
        body(
            &s,
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "asg2"),
                ("LaunchConfigurationName", "lc"),
                ("MinSize", "0"),
                ("MaxSize", "1"),
                ("AvailabilityZones.member.1", "us-east-1a"),
            ],
        );
        for (asg, val) in [("asg1", "a1"), ("asg2", "a2")] {
            body(
                &s,
                "CreateOrUpdateTags",
                &[
                    ("Tags.member.1.ResourceId", asg),
                    ("Tags.member.1.Key", "Name"),
                    ("Tags.member.1.Value", val),
                    ("Tags.member.1.PropagateAtLaunch", "true"),
                ],
            );
        }
        let d = body(
            &s,
            "DescribeTags",
            &[
                ("Filters.member.1.Name", "auto-scaling-group"),
                ("Filters.member.1.Values.member.1", "asg1"),
            ],
        );
        assert!(d.contains("<Value>a1</Value>"));
        assert!(!d.contains("<Value>a2</Value>"), "tag bleed: {d}");
    }

    #[test]
    fn describe_groups_paginates() {
        let s = svc();
        body(
            &s,
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc"),
                ("ImageId", "ami-1"),
                ("InstanceType", "t3.micro"),
            ],
        );
        for i in 0..5 {
            body(
                &s,
                "CreateAutoScalingGroup",
                &[
                    ("AutoScalingGroupName", &format!("g{i}")),
                    ("LaunchConfigurationName", "lc"),
                    ("MinSize", "0"),
                    ("MaxSize", "1"),
                    ("AvailabilityZones.member.1", "us-east-1a"),
                ],
            );
        }
        let page1 = body(&s, "DescribeAutoScalingGroups", &[("MaxRecords", "2")]);
        assert_eq!(
            page1.matches("<AutoScalingGroupName>").count(),
            2,
            "page1 should hold 2"
        );
        assert!(page1.contains("<NextToken>2</NextToken>"), "{page1}");
        let page3 = body(
            &s,
            "DescribeAutoScalingGroups",
            &[("MaxRecords", "2"), ("NextToken", "4")],
        );
        assert_eq!(page3.matches("<AutoScalingGroupName>").count(), 1);
        assert!(
            !page3.contains("<NextToken>"),
            "last page has no token: {page3}"
        );
    }

    #[test]
    fn china_region_arns_use_the_aws_cn_partition() {
        let s = svc();
        let call = |action: &str, params: &[(&str, &str)]| {
            let mut r = req(action, params);
            r.region = "cn-north-1".into();
            let resp = futures_block(s.handle(r)).unwrap();
            String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
        };
        call(
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc-cn"),
                ("ImageId", "ami-1"),
                ("InstanceType", "t3.micro"),
            ],
        );
        call(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "asg-cn"),
                ("LaunchConfigurationName", "lc-cn"),
                ("MinSize", "0"),
                ("MaxSize", "1"),
                ("DesiredCapacity", "0"),
                ("AvailabilityZones.member.1", "cn-north-1a"),
            ],
        );
        let lcs = call("DescribeLaunchConfigurations", &[]);
        assert!(
            lcs.contains("<LaunchConfigurationARN>arn:aws-cn:autoscaling:cn-north-1:123456789012:launchConfiguration:"),
            "{lcs}"
        );
        let groups = call("DescribeAutoScalingGroups", &[]);
        assert!(
            groups.contains("<AutoScalingGroupARN>arn:aws-cn:autoscaling:cn-north-1:123456789012:autoScalingGroup:"),
            "{groups}"
        );
        assert!(
            groups.contains("<ServiceLinkedRoleARN>arn:aws-cn:iam::123456789012:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling</ServiceLinkedRoleARN>"),
            "{groups}"
        );
    }

    fn ec2_wired() -> (AutoScalingService, fakecloud_ec2::SharedEc2State) {
        let ec2_state: fakecloud_ec2::SharedEc2State = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let s = AutoScalingService::new(Arc::new(parking_lot::RwLock::new(
            AutoScalingAccounts::new(),
        )))
        .with_ec2(ec2_state.clone(), None);
        (s, ec2_state)
    }

    async fn ec2_call(
        state: &fakecloud_ec2::SharedEc2State,
        action: &str,
        params: &[(&str, &str)],
    ) {
        let mut r = req(action, params);
        r.service = "ec2".into();
        fakecloud_ec2::Ec2Service::with_state(state.clone())
            .handle(r)
            .await
            .unwrap_or_else(|e| panic!("{action}: {}", e.message()));
    }

    /// The EC2 view of the group's instances: (instance type, image, tags,
    /// attached volumes as (size, encrypted)).
    type Ec2View = Vec<(String, String, Vec<(String, String)>, Vec<(i64, bool)>)>;

    fn ec2_view(state: &fakecloud_ec2::SharedEc2State, ids: &[String]) -> Ec2View {
        let accounts = state.read();
        let st = accounts.get("123456789012").unwrap();
        ids.iter()
            .map(|id| {
                let i = &st.instances[id];
                let tags = st
                    .tags_for(id)
                    .iter()
                    .map(|t| (t.key.clone(), t.value.clone()))
                    .collect();
                let vols = st
                    .volumes
                    .values()
                    .filter(|v| v.attachments.iter().any(|a| &a.instance_id == id))
                    .map(|v| (v.size, v.encrypted))
                    .collect();
                (i.instance_type.clone(), i.image_id.clone(), tags, vols)
            })
            .collect()
    }

    fn group_instance_ids(s: &AutoScalingService, name: &str) -> Vec<String> {
        s.state.read().accounts["123456789012"].groups[name]
            .instances
            .iter()
            .map(|i| i.instance_id.clone())
            .collect()
    }

    #[tokio::test]
    async fn launch_template_group_launches_template_volumes_and_tags() {
        let (s, ec2) = ec2_wired();
        ec2_call(
            &ec2,
            "CreateLaunchTemplate",
            &[
                ("LaunchTemplateName", "web"),
                ("LaunchTemplateData.ImageId", "ami-tmpl"),
                ("LaunchTemplateData.InstanceType", "t3.small"),
                (
                    "LaunchTemplateData.BlockDeviceMapping.1.DeviceName",
                    "/dev/xvda",
                ),
                (
                    "LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize",
                    "40",
                ),
                (
                    "LaunchTemplateData.BlockDeviceMapping.1.Ebs.Encrypted",
                    "true",
                ),
                (
                    "LaunchTemplateData.TagSpecification.1.ResourceType",
                    "instance",
                ),
                ("LaunchTemplateData.TagSpecification.1.Tag.1.Key", "team"),
                ("LaunchTemplateData.TagSpecification.1.Tag.1.Value", "tmpl"),
            ],
        )
        .await;
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                ("LaunchTemplate.LaunchTemplateName", "web"),
                ("MinSize", "2"),
                ("MaxSize", "2"),
                ("Tags.member.1.Key", "team"),
                ("Tags.member.1.Value", "asg"),
                ("Tags.member.1.PropagateAtLaunch", "true"),
                ("Tags.member.2.Key", "private"),
                ("Tags.member.2.Value", "x"),
                ("Tags.member.2.PropagateAtLaunch", "false"),
            ],
        ))
        .await
        .unwrap();
        let ids = group_instance_ids(&s, "g");
        assert_eq!(ids.len(), 2);
        for (itype, image, tags, vols) in ec2_view(&ec2, &ids) {
            assert_eq!(itype, "t3.small");
            assert_eq!(image, "ami-tmpl");
            assert_eq!(vols, vec![(40, true)], "template BDM volume");
            assert!(
                tags.contains(&("team".into(), "asg".into())),
                "group tag wins: {tags:?}"
            );
            assert!(tags.contains(&("aws:autoscaling:groupName".into(), "g".into())));
            assert!(tags.contains(&("aws:ec2launchtemplate:version".into(), "1".into())));
            assert!(!tags.iter().any(|(k, _)| k == "private"), "not propagated");
        }
        // The group reports the template by id + name with the default
        // version, and each instance the concrete version it came from.
        let desc = body_async(&s, "DescribeAutoScalingGroups", &[]).await;
        assert!(
            desc.contains("<LaunchTemplateName>web</LaunchTemplateName>"),
            "{desc}"
        );
        assert!(desc.contains("<Version>$Default</Version>"), "{desc}");
        assert!(desc.contains("<Version>1</Version>"), "{desc}");
        assert!(
            desc.contains("<InstanceType>t3.small</InstanceType>"),
            "{desc}"
        );
    }

    async fn body_async(s: &AutoScalingService, action: &str, params: &[(&str, &str)]) -> String {
        let r = s.handle(req(action, params)).await.unwrap();
        String::from_utf8_lossy(r.body.expect_bytes()).to_string()
    }

    #[tokio::test]
    async fn launch_configuration_group_gets_block_device_volumes_and_profile() {
        let (s, ec2) = ec2_wired();
        s.handle(req(
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc"),
                ("ImageId", "ami-lc"),
                ("InstanceType", "m5.large"),
                ("IamInstanceProfile", "lc-profile"),
                ("BlockDeviceMappings.member.1.DeviceName", "/dev/xvda"),
                ("BlockDeviceMappings.member.1.Ebs.VolumeSize", "16"),
                ("BlockDeviceMappings.member.1.Ebs.Encrypted", "true"),
                ("BlockDeviceMappings.member.2.DeviceName", "/dev/xvdb"),
                ("BlockDeviceMappings.member.2.Ebs.VolumeSize", "100"),
            ],
        ))
        .await
        .unwrap();
        let lcs = body_async(&s, "DescribeLaunchConfigurations", &[]).await;
        assert!(
            lcs.contains("<DeviceName>/dev/xvdb</DeviceName><Ebs><VolumeSize>100</VolumeSize>"),
            "{lcs}"
        );
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                ("LaunchConfigurationName", "lc"),
                ("MinSize", "1"),
                ("MaxSize", "1"),
            ],
        ))
        .await
        .unwrap();
        let ids = group_instance_ids(&s, "g");
        let view = ec2_view(&ec2, &ids);
        assert_eq!(view.len(), 1);
        let (itype, image, _, mut vols) = view[0].clone();
        vols.sort();
        assert_eq!((itype.as_str(), image.as_str()), ("m5.large", "ami-lc"));
        assert_eq!(vols, vec![(16, true), (100, false)]);
        let accounts = ec2.read();
        assert!(accounts
            .get("123456789012")
            .unwrap()
            .iam_instance_profile_associations
            .values()
            .any(|a| a.instance_id == ids[0]
                && a.iam_instance_profile_arn
                    .ends_with("instance-profile/lc-profile")));
    }

    #[tokio::test]
    async fn mixed_instances_policy_launches_override_type_and_weight() {
        let (s, ec2) = ec2_wired();
        ec2_call(
            &ec2,
            "CreateLaunchTemplate",
            &[
                ("LaunchTemplateName", "base"),
                ("LaunchTemplateData.ImageId", "ami-base"),
                ("LaunchTemplateData.InstanceType", "t3.micro"),
            ],
        )
        .await;
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                (
                    "MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName",
                    "base",
                ),
                ("MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.Version", "$Latest"),
                ("MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.InstanceType", "c5.xlarge"),
                ("MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.WeightedCapacity", "2"),
                ("MixedInstancesPolicy.LaunchTemplate.Overrides.member.2.InstanceType", "m5.large"),
                ("MinSize", "4"),
                ("MaxSize", "4"),
            ],
        ))
        .await
        .unwrap();
        // Desired 4 capacity units at weight 2 -> two c5.xlarge instances.
        let ids = group_instance_ids(&s, "g");
        assert_eq!(ids.len(), 2);
        for (itype, image, _, _) in ec2_view(&ec2, &ids) {
            assert_eq!((itype.as_str(), image.as_str()), ("c5.xlarge", "ami-base"));
        }
        let desc = body_async(&s, "DescribeAutoScalingGroups", &[]).await;
        assert!(desc.contains("<MixedInstancesPolicy>"), "{desc}");
        assert!(
            desc.contains("<WeightedCapacity>2</WeightedCapacity>"),
            "{desc}"
        );
    }

    #[tokio::test]
    async fn unknown_launch_template_or_configuration_is_rejected() {
        let (s, _) = ec2_wired();
        let err = s
            .handle(req(
                "CreateAutoScalingGroup",
                &[
                    ("AutoScalingGroupName", "g"),
                    ("LaunchTemplate.LaunchTemplateName", "missing"),
                    ("MinSize", "1"),
                    ("MaxSize", "1"),
                ],
            ))
            .await
            .err()
            .expect("unknown launch template rejected");
        assert_eq!(err.code(), "ValidationError");
        assert!(
            err.message().contains("valid fully-formed launch template"),
            "{}",
            err.message()
        );
        let err = s
            .handle(req(
                "CreateAutoScalingGroup",
                &[
                    ("AutoScalingGroupName", "g"),
                    ("LaunchConfigurationName", "missing"),
                    ("MinSize", "1"),
                    ("MaxSize", "1"),
                ],
            ))
            .await
            .err()
            .expect("unknown launch configuration rejected");
        assert_eq!(err.code(), "ValidationError");
    }

    #[tokio::test]
    async fn deleted_template_version_records_a_failed_launch() {
        let (s, ec2) = ec2_wired();
        ec2_call(
            &ec2,
            "CreateLaunchTemplate",
            &[
                ("LaunchTemplateName", "web"),
                ("LaunchTemplateData.ImageId", "ami-1"),
            ],
        )
        .await;
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                ("LaunchTemplate.LaunchTemplateName", "web"),
                ("MinSize", "0"),
                ("MaxSize", "2"),
                ("DesiredCapacity", "0"),
            ],
        ))
        .await
        .unwrap();
        ec2_call(
            &ec2,
            "DeleteLaunchTemplate",
            &[("LaunchTemplateName", "web")],
        )
        .await;
        s.handle(req(
            "SetDesiredCapacity",
            &[("AutoScalingGroupName", "g"), ("DesiredCapacity", "1")],
        ))
        .await
        .unwrap();
        assert!(
            group_instance_ids(&s, "g").is_empty(),
            "no phantom instance"
        );
        let acts = body_async(
            &s,
            "DescribeScalingActivities",
            &[("AutoScalingGroupName", "g")],
        )
        .await;
        assert!(acts.contains("<StatusCode>Failed</StatusCode>"), "{acts}");
        assert!(acts.contains("does not exist"), "{acts}");
    }

    #[tokio::test]
    async fn instance_id_group_launches_like_the_instance() {
        let (s, ec2) = ec2_wired();
        let mut r = req(
            "RunInstances",
            &[
                ("ImageId", "ami-src"),
                ("InstanceType", "c5.large"),
                ("MinCount", "1"),
                ("MaxCount", "1"),
                ("BlockDeviceMapping.1.DeviceName", "/dev/xvda"),
                ("BlockDeviceMapping.1.Ebs.VolumeSize", "9"),
            ],
        );
        r.service = "ec2".into();
        let out = fakecloud_ec2::Ec2Service::with_state(ec2.clone())
            .handle(r)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(out.body.expect_bytes()).to_string();
        let src = parse_instance_ids(&body)[0].clone();
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                ("InstanceId", &src),
                ("MinSize", "1"),
                ("MaxSize", "1"),
            ],
        ))
        .await
        .unwrap();
        let ids = group_instance_ids(&s, "g");
        assert_eq!(ids.len(), 1);
        let view = ec2_view(&ec2, &ids);
        assert_eq!(
            (view[0].0.as_str(), view[0].1.as_str()),
            ("c5.large", "ami-src")
        );
        assert_eq!(view[0].3, vec![(9, false)]);
        // The derived launch configuration is named after the group.
        let lcs = body_async(&s, "DescribeLaunchConfigurations", &[]).await;
        assert!(
            lcs.contains("<LaunchConfigurationName>g</LaunchConfigurationName>"),
            "{lcs}"
        );
    }

    #[test]
    fn weighted_scale_in_reaches_the_target() {
        let pick = |v: &[(&str, i64)], t| {
            scale_in_choice(v.iter().map(|(i, w)| (i.to_string(), *w)).collect(), t)
        };
        // Weights 2 then 1, target 1: drop the weight-2 instance, not the newest.
        assert_eq!(pick(&[("a", 2), ("b", 1)], 1), vec!["a"]);
        // Unweighted: newest first.
        assert_eq!(pick(&[("a", 1), ("b", 1), ("c", 1)], 1), vec!["c", "b"]);
    }

    #[tokio::test]
    async fn update_rejects_two_launch_sources() {
        let (s, ec2) = ec2_wired();
        ec2_call(
            &ec2,
            "CreateLaunchTemplate",
            &[
                ("LaunchTemplateName", "web"),
                ("LaunchTemplateData.ImageId", "ami-1"),
            ],
        )
        .await;
        s.handle(req(
            "CreateLaunchConfiguration",
            &[
                ("LaunchConfigurationName", "lc"),
                ("ImageId", "ami-1"),
                ("InstanceType", "t3.micro"),
            ],
        ))
        .await
        .unwrap();
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                ("LaunchConfigurationName", "lc"),
                ("MinSize", "0"),
                ("MaxSize", "1"),
            ],
        ))
        .await
        .unwrap();
        let err = s
            .handle(req(
                "UpdateAutoScalingGroup",
                &[
                    ("AutoScalingGroupName", "g"),
                    ("LaunchConfigurationName", "lc"),
                    ("LaunchTemplate.LaunchTemplateName", "web"),
                ],
            ))
            .await
            .err()
            .expect("two sources rejected");
        assert_eq!(err.code(), "ValidationError");
    }

    #[tokio::test]
    async fn mixed_spot_share_launches_spot_instances() {
        let (s, ec2) = ec2_wired();
        ec2_call(
            &ec2,
            "CreateLaunchTemplate",
            &[
                ("LaunchTemplateName", "base"),
                ("LaunchTemplateData.ImageId", "ami-1"),
            ],
        )
        .await;
        let p = "MixedInstancesPolicy";
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                (
                    &format!("{p}.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName"),
                    "base",
                ),
                (
                    &format!("{p}.LaunchTemplate.Overrides.member.1.InstanceType"),
                    "c5.large",
                ),
                (
                    &format!("{p}.LaunchTemplate.Overrides.member.2.InstanceType"),
                    "m5.large",
                ),
                (
                    &format!("{p}.InstancesDistribution.OnDemandBaseCapacity"),
                    "1",
                ),
                (
                    &format!("{p}.InstancesDistribution.OnDemandPercentageAboveBaseCapacity"),
                    "0",
                ),
                (
                    &format!("{p}.InstancesDistribution.SpotAllocationStrategy"),
                    "capacity-optimized",
                ),
                ("MinSize", "3"),
                ("MaxSize", "3"),
            ],
        ))
        .await
        .unwrap();
        let ids = group_instance_ids(&s, "g");
        let accounts = ec2.read();
        let st = accounts.get("123456789012").unwrap();
        let mut got: Vec<(String, Option<String>)> = ids
            .iter()
            .map(|id| {
                let i = &st.instances[id];
                (i.instance_type.clone(), i.instance_lifecycle.clone())
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("c5.large".to_string(), None),
                ("c5.large".to_string(), Some("spot".to_string())),
                ("m5.large".to_string(), Some("spot".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn spot_launch_template_records_spot_instances() {
        let (s, ec2) = ec2_wired();
        ec2_call(
            &ec2,
            "CreateLaunchTemplate",
            &[
                ("LaunchTemplateName", "spot"),
                ("LaunchTemplateData.ImageId", "ami-1"),
                (
                    "LaunchTemplateData.InstanceMarketOptions.MarketType",
                    "spot",
                ),
            ],
        )
        .await;
        s.handle(req(
            "CreateAutoScalingGroup",
            &[
                ("AutoScalingGroupName", "g"),
                ("LaunchTemplate.LaunchTemplateName", "spot"),
                ("MinSize", "1"),
                ("MaxSize", "1"),
            ],
        ))
        .await
        .unwrap();
        let st = s.state.read();
        let inst = &st.accounts["123456789012"].groups["g"].instances[0];
        assert_eq!(inst.lifecycle.as_deref(), Some("spot"));
    }
}
