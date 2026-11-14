//! Response builders, validators, and JSON serializers for the EKS service.
//!
//! Split out of `service.rs` (which retains the request handlers and the
//! `AwsService` impl) purely to keep each file focused; there is no behavior
//! change. Everything here is `pub(crate)` so `service.rs` can call it.

use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsServiceError};

pub(crate) use crate::addon_catalog::{
    addon_catalog, pod_identity_configuration, resolve_addon_version,
};
use crate::service::LOG_TYPES;
use crate::state::*;

pub(crate) fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

pub(crate) fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

pub(crate) fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// Validate the shared `maxResults` query param (range 1-100) used by the list
/// operations, returning the effective page size.
pub(crate) fn validate_max_results(req: &AwsRequest) -> Result<usize, AwsServiceError> {
    match req.query_params.get("maxResults") {
        Some(raw) => {
            let n: i64 = raw
                .parse()
                .map_err(|_| invalid_parameter("maxResults must be an integer"))?;
            if !(1..=100).contains(&n) {
                return Err(invalid_parameter("maxResults must be between 1 and 100"));
            }
            Ok(n as usize)
        }
        None => Ok(100),
    }
}

pub(crate) fn validate_cluster_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 100 {
        return Err(invalid_parameter("name must be 1-100 characters"));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphanumeric() {
        return Err(invalid_parameter(
            "name must start with an alphanumeric character",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(invalid_parameter(
            "name must match ^[0-9A-Za-z][A-Za-z0-9\\-_]*$",
        ));
    }
    Ok(())
}

pub(crate) fn not_found_cluster(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No cluster found for name: {name}."),
        )
    }
}

pub(crate) fn not_found_update(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No update found for id: {id}."),
        )
    }
}

pub(crate) fn not_found_nodegroup(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No node group found for name: {name}."),
        )
    }
}

pub(crate) fn not_found_fargate_profile(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No Fargate Profile found with name: {name}."),
        )
    }
}

pub(crate) fn not_found_addon(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No addon found for name: {name}."),
        )
    }
}

pub(crate) fn not_found_access_entry(
    principal_arn: &str,
) -> impl Fn() -> AwsServiceError + 'static {
    let principal_arn = principal_arn.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No access entry found for principal ARN: {principal_arn}."),
        )
    }
}

pub(crate) fn not_found_identity_provider_config(
    name: &str,
) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No identity provider config found for name: {name}."),
        )
    }
}

pub(crate) fn not_found_pod_identity_association(
    id: &str,
) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No pod identity association found for id: {id}."),
        )
    }
}

/// Validate that a string is a well-formed EKS ARN, rejecting anything else
/// with a BadRequestException.
pub(crate) fn validate_eks_arn(arn: &str) -> Result<(), AwsServiceError> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "eks" {
        return Err(bad_request(format!("Invalid EKS ARN: {arn}")));
    }
    Ok(())
}

/// Which resource a tagging ARN points at. Located by scanning the stored
/// resources for a matching ARN (rather than reverse-parsing every ARN shape),
/// so the mutable borrow can be taken precisely afterwards.
pub(crate) enum TagTarget {
    Cluster(String),
    Nodegroup(String, String),
    Fargate(String, String),
    Addon(String, String),
    AccessEntry(String, String),
    Idp(String, String),
    PodIdentity(String, String),
    Capability(String, String),
    Subscription(String),
    /// An ARN that matched no tracked resource; tags are kept in the account's
    /// side `tags` map keyed by the ARN.
    Generic(String),
}

pub(crate) fn locate_tag_target(state: &crate::state::EksState, arn: &str) -> TagTarget {
    if let Some((n, _)) = state.clusters.iter().find(|(_, c)| c.arn == arn) {
        return TagTarget::Cluster(n.clone());
    }
    for (cn, m) in &state.nodegroups {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Nodegroup(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.fargate_profiles {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Fargate(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.addons {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Addon(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.access_entries {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::AccessEntry(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.identity_provider_configs {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Idp(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.pod_identity_associations {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.association_arn == arn) {
            return TagTarget::PodIdentity(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.capabilities {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Capability(cn.clone(), k.clone());
        }
    }
    if let Some((k, _)) = state
        .eks_anywhere_subscriptions
        .iter()
        .find(|(_, r)| r.arn == arn)
    {
        return TagTarget::Subscription(k.clone());
    }
    TagTarget::Generic(arn.to_string())
}

pub(crate) fn tags_mut<'a>(
    state: &'a mut crate::state::EksState,
    target: &TagTarget,
) -> &'a mut crate::state::TagMap {
    // Every arm but `Generic` was just located under the same write lock, so the
    // lookups cannot miss.
    match target {
        TagTarget::Cluster(n) => &mut state.clusters.get_mut(n).unwrap().tags,
        TagTarget::Nodegroup(c, k) => {
            &mut state
                .nodegroups
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Fargate(c, k) => {
            &mut state
                .fargate_profiles
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Addon(c, k) => &mut state.addons.get_mut(c).unwrap().get_mut(k).unwrap().tags,
        TagTarget::AccessEntry(c, k) => {
            &mut state
                .access_entries
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Idp(c, k) => {
            &mut state
                .identity_provider_configs
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::PodIdentity(c, k) => {
            &mut state
                .pod_identity_associations
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Capability(c, k) => {
            &mut state
                .capabilities
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Subscription(k) => {
            &mut state.eks_anywhere_subscriptions.get_mut(k).unwrap().tags
        }
        TagTarget::Generic(a) => state.tags.entry(a.clone()).or_default(),
    }
}

pub(crate) fn tags_ref<'a>(
    state: &'a crate::state::EksState,
    target: &TagTarget,
) -> Option<&'a crate::state::TagMap> {
    match target {
        TagTarget::Cluster(n) => state.clusters.get(n).map(|c| &c.tags),
        TagTarget::Nodegroup(c, k) => state
            .nodegroups
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Fargate(c, k) => state
            .fargate_profiles
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Addon(c, k) => state.addons.get(c).and_then(|m| m.get(k)).map(|r| &r.tags),
        TagTarget::AccessEntry(c, k) => state
            .access_entries
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Idp(c, k) => state
            .identity_provider_configs
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::PodIdentity(c, k) => state
            .pod_identity_associations
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Capability(c, k) => state
            .capabilities
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Subscription(k) => state.eks_anywhere_subscriptions.get(k).map(|r| &r.tags),
        TagTarget::Generic(a) => state.tags.get(a),
    }
}

pub(crate) fn parse_tag_map(v: Option<&Value>) -> crate::state::TagMap {
    let mut out = crate::state::TagMap::new();
    if let Some(obj) = v.and_then(|v| v.as_object()) {
        for (k, val) in obj {
            if let Some(s) = val.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Parse a repeated query-string key (`?tagKeys=a&tagKeys=b`) into decoded values.
pub(crate) fn parse_multi_query(raw_query: &str, key: &str) -> Vec<String> {
    let prefix = format!("{key}=");
    raw_query
        .split('&')
        .filter(|pair| pair.starts_with(&prefix))
        .map(|pair| decode(&pair[prefix.len()..]))
        .collect()
}

pub(crate) fn new_update(update_type: &str, params: Vec<(String, String)>) -> Update {
    Update {
        id: uuid::Uuid::new_v4().to_string(),
        status: "InProgress".to_string(),
        type_: update_type.to_string(),
        params,
        created_at: Utc::now(),
    }
}

pub(crate) fn timestamp_to_number(t: DateTime<Utc>) -> Value {
    let secs = t.timestamp() as f64;
    let frac = t.timestamp_subsec_millis() as f64 / 1000.0;
    Value::from(secs + frac)
}

/// Recover the synthetic cluster id from a stored endpoint URL so describe/delete
/// can re-derive the OIDC issuer without persisting the id separately.
pub(crate) fn arn_cluster_id(endpoint: &str) -> String {
    endpoint
        .strip_prefix("https://")
        .and_then(|s| s.split('.').next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}

pub(crate) fn default_ca_data() -> String {
    // A stable placeholder base64 blob; real clusters return a PEM-encoded CA.
    "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==".to_string()
}

pub(crate) fn build_vpc_config_response(req: &Value, id: &str) -> Value {
    let subnet_ids = req.get("subnetIds").cloned().unwrap_or_else(|| json!([]));
    let security_group_ids = req
        .get("securityGroupIds")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let public_access_cidrs = req
        .get("publicAccessCidrs")
        .cloned()
        .unwrap_or_else(|| json!(["0.0.0.0/0"]));
    let endpoint_public = req
        .get("endpointPublicAccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let endpoint_private = req
        .get("endpointPrivateAccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut out = json!({
        "subnetIds": subnet_ids,
        "securityGroupIds": security_group_ids,
        "clusterSecurityGroupId": format!("sg-{}", &id.replace('-', "")[..17.min(id.replace('-', "").len())]),
        "vpcId": format!("vpc-{}", &id.replace('-', "")[..17.min(id.replace('-', "").len())]),
        "endpointPublicAccess": endpoint_public,
        "endpointPrivateAccess": endpoint_private,
        "publicAccessCidrs": public_access_cidrs,
    });
    if let Some(mode) = req.get("controlPlaneEgressMode") {
        out["controlPlaneEgressMode"] = mode.clone();
    }
    out
}

/// Apply an `UpdateClusterConfig` `resourcesVpcConfig` to the stored
/// `VpcConfigResponse`: only the members present in the request change. The
/// cluster's VPC and its EKS-managed security group are fixed at creation and
/// are never replaced.
pub(crate) fn merge_vpc_config_update(existing: &mut Value, req: &Value) {
    for key in [
        "subnetIds",
        "securityGroupIds",
        "endpointPublicAccess",
        "endpointPrivateAccess",
        "publicAccessCidrs",
        "controlPlaneEgressMode",
    ] {
        if let Some(v) = req.get(key) {
            existing[key] = v.clone();
        }
    }
}

pub(crate) fn build_k8s_network_config(req: Option<&Value>) -> Value {
    let ip_family = req
        .and_then(|v| v.get("ipFamily"))
        .and_then(|v| v.as_str())
        .unwrap_or("ipv4")
        .to_string();
    let service_cidr = req
        .and_then(|v| v.get("serviceIpv4Cidr"))
        .and_then(|v| v.as_str())
        .unwrap_or("172.20.0.0/16")
        .to_string();
    // `elasticLoadBalancing` is always present in the response; `enabled`
    // defaults to false unless the caller opts in (auto mode / EKS Auto).
    let elb_enabled = req
        .and_then(|v| v.get("elasticLoadBalancing"))
        .and_then(|v| v.get("enabled"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut elb = json!({ "enabled": elb_enabled });
    if let Some(gw) = req
        .and_then(|v| v.get("elasticLoadBalancing"))
        .and_then(|v| v.get("gatewayApi"))
        .filter(|v| v.is_object())
    {
        elb["gatewayApi"] = gw.clone();
    }
    let mut out = json!({
        "serviceIpv4Cidr": service_cidr,
        "ipFamily": ip_family,
        "elasticLoadBalancing": elb,
    });
    // An IPv6 cluster gets a service CIDR carved from a unique-local range
    // (EKS assigns a /108 from fc00::/7).
    if ip_family == "ipv6" {
        let id = uuid::Uuid::new_v4().simple().to_string();
        out["serviceIpv6Cidr"] = json!(format!(
            "fd{}:{}:{}::/108",
            &id[0..2],
            &id[2..6],
            &id[6..10]
        ));
    }
    out
}

/// Apply an `UpdateClusterConfig` `kubernetesNetworkConfig` to the stored
/// config: only the members present in the request change, so toggling
/// `elasticLoadBalancing` keeps the cluster's service CIDR and IP family.
pub(crate) fn merge_k8s_network_config_update(existing: &mut Value, req: &Value) {
    if !existing.is_object() {
        *existing = build_k8s_network_config(None);
    }
    for key in ["serviceIpv4Cidr", "ipFamily"] {
        if let Some(v) = req.get(key).filter(|v| v.is_string()) {
            existing[key] = v.clone();
        }
    }
    if let Some(req_elb) = req.get("elasticLoadBalancing").filter(|v| v.is_object()) {
        if !existing["elasticLoadBalancing"].is_object() {
            existing["elasticLoadBalancing"] = json!({ "enabled": false });
        }
        if let Some(enabled) = req_elb.get("enabled").and_then(Value::as_bool) {
            existing["elasticLoadBalancing"]["enabled"] = json!(enabled);
        }
        if let Some(gw) = req_elb.get("gatewayApi").filter(|v| v.is_object()) {
            existing["elasticLoadBalancing"]["gatewayApi"] = gw.clone();
        }
    }
}

/// Apply an `UpdateNodegroupVersion` `launchTemplate` to a node group: the
/// node group keeps its launch template (a given `id` / `name` must match it)
/// and moves to the requested `version`.
pub(crate) fn apply_launch_template_version(
    ng: &mut crate::state::Nodegroup,
    req: &Value,
) -> Result<(), fakecloud_core::service::AwsServiceError> {
    let Some(current) = ng.launch_template.as_mut().filter(|v| v.is_object()) else {
        return Err(invalid_parameter(
            "You cannot specify a launch template for a node group that was not created with one",
        ));
    };
    for key in ["id", "name"] {
        if let (Some(want), Some(have)) = (
            req.get(key).and_then(Value::as_str),
            current.get(key).and_then(Value::as_str),
        ) {
            if want != have {
                return Err(invalid_parameter(format!(
                    "Launch template {key} '{want}' does not match the node group's launch template {key} '{have}'"
                )));
            }
        }
    }
    for key in ["id", "name", "version"] {
        if let Some(v) = req.get(key).filter(|v| v.is_string()) {
            current[key] = v.clone();
        }
    }
    Ok(())
}

/// An add-on manages the pod identity associations it owns; they can only be
/// changed or removed through the add-on (`UpdateAddon` / `DeleteAddon`), so
/// a direct Update/DeletePodIdentityAssociation is refused, as on AWS.
pub(crate) fn reject_addon_owned_association(
    a: &PodIdentityAssociation,
) -> Result<(), AwsServiceError> {
    match &a.owner_arn {
        Some(owner) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequestException",
            format!(
                "Pod Identity association {} is owned by {owner}. Update or delete the add-on to change it.",
                a.association_id
            ),
        )),
        None => Ok(()),
    }
}

/// Build an `AccessConfigResponse` object. The API only reports
/// `authenticationMode` (bootstrap-creator permission is a create-only input),
/// defaulting to `CONFIG_MAP` when the caller omits `accessConfig`.
pub(crate) fn build_access_config(req: Option<&Value>) -> Value {
    let mode = req
        .and_then(|v| v.get("authenticationMode"))
        .and_then(|v| v.as_str())
        .unwrap_or("CONFIG_MAP")
        .to_string();
    json!({ "authenticationMode": mode })
}

/// Build an `UpgradePolicyResponse` object, defaulting `supportType` to
/// `EXTENDED` (extended support enabled) when the caller omits `upgradePolicy`.
pub(crate) fn build_upgrade_policy(req: Option<&Value>) -> Value {
    let support = req
        .and_then(|v| v.get("supportType"))
        .and_then(|v| v.as_str())
        .unwrap_or("EXTENDED")
        .to_string();
    json!({ "supportType": support })
}

pub(crate) fn build_logging(req: Option<&Value>) -> Value {
    // If the caller supplies a Logging block, echo its clusterLogging; else
    // default to all types disabled (what AWS reports for a fresh cluster).
    if let Some(setups) = req
        .and_then(|v| v.get("clusterLogging"))
        .and_then(|v| v.as_array())
    {
        return json!({ "clusterLogging": setups });
    }
    json!({
        "clusterLogging": [{
            "types": LOG_TYPES,
            "enabled": false,
        }],
    })
}

pub(crate) fn cluster_json(c: &Cluster, id: &str) -> Value {
    // Real AWS scopes the OIDC issuer host to the cluster's region:
    // `https://oidc.eks.<region>.amazonaws.com/id/<ID>`. Tooling (eksctl IRSA,
    // Terraform `aws_iam_openid_connect_provider { url = ...oidc[0].issuer }`)
    // parses `<region>` out of the host, so a region-less issuer breaks on any
    // non-default region. The cluster's region is embedded in its ARN
    // (`arn:PARTITION:eks:REGION:ACCOUNT:cluster/NAME`) at field index 3.
    let region = c
        .arn
        .split(':')
        .nth(3)
        .filter(|s| !s.is_empty())
        .unwrap_or("us-east-1");
    let mut out = json!({
        "name": c.name,
        "arn": c.arn,
        "createdAt": timestamp_to_number(c.created_at),
        "version": c.version,
        "endpoint": c.endpoint,
        "roleArn": c.role_arn,
        "resourcesVpcConfig": c.resources_vpc_config,
        "kubernetesNetworkConfig": c.kubernetes_network_config,
        "logging": c.logging,
        "identity": {
            "oidc": {
                "issuer": format!(
                    "https://oidc.eks.{region}.amazonaws.com/id/{}",
                    id.to_uppercase()
                ),
            },
        },
        "status": c.status,
        "certificateAuthority": { "data": c.certificate_authority_data },
        "platformVersion": c.platform_version,
        "tags": c.tags,
        "health": { "issues": [] },
        "accessConfig": c.access_config,
        "upgradePolicy": c.upgrade_policy,
    });
    if let Some(cc) = &c.connector_config {
        out["connectorConfig"] = cc.clone();
    }
    if let Some(ec) = &c.encryption_config {
        out["encryptionConfig"] = ec.clone();
    }
    if let Some(v) = &c.compute_config {
        out["computeConfig"] = v.clone();
    }
    if let Some(v) = &c.storage_config {
        out["storageConfig"] = v.clone();
    }
    if let Some(v) = &c.zonal_shift_config {
        out["zonalShiftConfig"] = v.clone();
    }
    if let Some(v) = &c.remote_network_config {
        out["remoteNetworkConfig"] = v.clone();
    }
    if let Some(v) = &c.control_plane_scaling_config {
        out["controlPlaneScalingConfig"] = v.clone();
    }
    if let Some(v) = c.deletion_protection {
        out["deletionProtection"] = Value::Bool(v);
    }
    out
}

pub(crate) fn update_json(u: &Update) -> Value {
    let params: Vec<Value> = u
        .params
        .iter()
        .map(|(t, v)| json!({ "type": t, "value": v }))
        .collect();
    json!({
        "id": u.id,
        "status": u.status,
        "type": u.type_,
        "params": params,
        "createdAt": timestamp_to_number(u.created_at),
        "errors": [],
    })
}

/// Build a `NodegroupScalingConfig`, defaulting to AWS's create-time defaults
/// (min 1 / max 2 / desired 2) for any member the caller omits.
pub(crate) fn build_scaling_config(req: Option<&Value>) -> Value {
    let min = req
        .and_then(|v| v.get("minSize"))
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    let max = req
        .and_then(|v| v.get("maxSize"))
        .and_then(|v| v.as_i64())
        .unwrap_or(2);
    let desired = req
        .and_then(|v| v.get("desiredSize"))
        .and_then(|v| v.as_i64())
        .unwrap_or(2);
    json!({ "minSize": min, "maxSize": max, "desiredSize": desired })
}

/// Identity of a Kubernetes taint for add/update/remove matching: taints are
/// keyed by `key` + `effect` (value can change on an update-in-place).
pub(crate) fn taint_identity(t: &Value) -> (Option<&str>, Option<&str>) {
    (
        t.get("key").and_then(|v| v.as_str()),
        t.get("effect").and_then(|v| v.as_str()),
    )
}

/// Build a `NodegroupUpdateConfig`, defaulting `maxUnavailable` to 1.
pub(crate) fn build_nodegroup_update_config(req: Option<&Value>) -> Value {
    if let Some(pct) = req
        .and_then(|v| v.get("maxUnavailablePercentage"))
        .and_then(|v| v.as_i64())
    {
        return json!({ "maxUnavailablePercentage": pct });
    }
    let max_unavailable = req
        .and_then(|v| v.get("maxUnavailable"))
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    json!({ "maxUnavailable": max_unavailable })
}

pub(crate) fn nodegroup_json(n: &Nodegroup) -> Value {
    let mut out = json!({
        "nodegroupName": n.name,
        "nodegroupArn": n.arn,
        "clusterName": n.cluster_name,
        "version": n.version,
        "releaseVersion": n.release_version,
        "createdAt": timestamp_to_number(n.created_at),
        "modifiedAt": timestamp_to_number(n.modified_at),
        "status": n.status,
        "capacityType": n.capacity_type,
        "scalingConfig": n.scaling_config,
        "instanceTypes": n.instance_types,
        "subnets": n.subnets,
        "amiType": n.ami_type,
        "nodeRole": n.node_role,
        "labels": n.labels,
        "taints": n.taints,
        "resources": {
            "autoScalingGroups": [{ "name": n.asg_name }],
        },
        "diskSize": n.disk_size,
        "health": { "issues": [] },
        "updateConfig": n.update_config,
        "tags": n.tags,
    });
    if let Some(ra) = &n.remote_access {
        out["remoteAccess"] = ra.clone();
    }
    if let Some(lt) = &n.launch_template {
        out["launchTemplate"] = lt.clone();
    }
    if let Some(nrc) = &n.node_repair_config {
        out["nodeRepairConfig"] = nrc.clone();
    }
    if let Some(wpc) = &n.warm_pool_config {
        out["warmPoolConfig"] = wpc.clone();
    }
    out
}

pub(crate) fn fargate_profile_json(p: &FargateProfile) -> Value {
    json!({
        "fargateProfileName": p.name,
        "fargateProfileArn": p.arn,
        "clusterName": p.cluster_name,
        "createdAt": timestamp_to_number(p.created_at),
        "podExecutionRoleArn": p.pod_execution_role_arn,
        "subnets": p.subnets,
        "selectors": p.selectors,
        "status": p.status,
        "tags": p.tags,
        "health": { "issues": [] },
    })
}

pub(crate) fn addon_json(a: &Addon) -> Value {
    let mut out = json!({
        "addonName": a.name,
        "clusterName": a.cluster_name,
        "status": a.status,
        "addonVersion": a.addon_version,
        "addonArn": a.arn,
        "createdAt": timestamp_to_number(a.created_at),
        "modifiedAt": timestamp_to_number(a.modified_at),
        "tags": a.tags,
        "health": { "issues": [] },
    });
    if let Some(role) = &a.service_account_role_arn {
        out["serviceAccountRoleArn"] = Value::String(role.clone());
    }
    if let Some(cfg) = &a.configuration_values {
        out["configurationValues"] = Value::String(cfg.clone());
    }
    if let Some(ns) = &a.namespace {
        out["namespaceConfig"] = json!({ "namespace": ns });
    }
    if !a.pod_identity_associations.is_empty() {
        out["podIdentityAssociations"] = json!(a.pod_identity_associations);
    }
    out
}

/// A JSON-schema string describing the configuration accepted by an add-on,
/// returned by `DescribeAddonConfiguration`.
pub(crate) fn addon_configuration_schema(addon_name: &str) -> String {
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": format!("{addon_name} configuration schema"),
        "additionalProperties": false,
        "properties": {
            "resources": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "limits": { "type": "object" },
                    "requests": { "type": "object" },
                },
            },
            "tolerations": { "type": "array" },
            "nodeSelector": { "type": "object" },
        },
    })
    .to_string()
}

/// Split a principal ARN into its access-entry `(type, name)` ARN segments.
/// `arn:aws:iam::123:role/Foo` -> `("role", "Foo")`; users map to `user`;
/// anything else (root, assumed-role, unrecognised) falls back to `standard`.
pub(crate) fn principal_parts(principal_arn: &str) -> (String, String) {
    let resource = principal_arn
        .split(':')
        .next_back()
        .unwrap_or(principal_arn);
    let (kind, name) = resource.split_once('/').unwrap_or(("", resource));
    let name = name.rsplit('/').next().unwrap_or(name);
    let type_seg = match kind {
        "role" | "assumed-role" => "role",
        "user" => "user",
        _ => "standard",
    };
    (type_seg.to_string(), name.to_string())
}

/// The default `username` EKS derives for an access entry when the caller omits
/// one: `arn:aws:iam::123:role/Foo` -> a role SessionName template that mirrors
/// what AWS records for a role principal.
pub(crate) fn default_username(principal_arn: &str) -> String {
    let (type_seg, name) = principal_parts(principal_arn);
    match type_seg.as_str() {
        "role" => {
            let partition = fakecloud_aws::arn::partition_of(principal_arn);
            format!(
                "arn:{partition}:sts::{{{{AccountID}}}}:assumed-role/{name}/{{{{SessionName}}}}"
            )
        }
        _ => principal_arn.to_string(),
    }
}

/// Parse a `StringList` request member into a `Vec<String>`.
pub(crate) fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Build an `AccessScope` response object, defaulting `type` to `cluster` (the
/// AWS default) and echoing any supplied namespaces.
pub(crate) fn build_access_scope(req: Option<&Value>) -> Value {
    let scope_type = req
        .and_then(|v| v.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("cluster")
        .to_string();
    let namespaces = req
        .and_then(|v| v.get("namespaces"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    json!({ "type": scope_type, "namespaces": namespaces })
}

pub(crate) fn access_entry_json(e: &AccessEntry) -> Value {
    json!({
        "clusterName": e.cluster_name,
        "principalArn": e.principal_arn,
        "kubernetesGroups": e.kubernetes_groups,
        "accessEntryArn": e.arn,
        "createdAt": timestamp_to_number(e.created_at),
        "modifiedAt": timestamp_to_number(e.modified_at),
        "tags": e.tags,
        "username": e.username,
        "type": e.type_,
    })
}

pub(crate) fn associated_policy_json(ap: &AssociatedPolicy) -> Value {
    json!({
        "policyArn": ap.policy_arn,
        "accessScope": ap.access_scope,
        "associatedAt": timestamp_to_number(ap.associated_at),
        "modifiedAt": timestamp_to_number(ap.modified_at),
    })
}

/// Read an optional string member from a JSON object.
pub(crate) fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
}

pub(crate) fn identity_provider_config_json(c: &IdentityProviderConfig) -> Value {
    let mut oidc = json!({
        "identityProviderConfigName": c.name,
        "identityProviderConfigArn": c.arn,
        "clusterName": c.cluster_name,
        "issuerUrl": c.issuer_url,
        "clientId": c.client_id,
        "requiredClaims": c.required_claims,
        "status": c.status,
        "tags": c.tags,
    });
    if let Some(v) = &c.username_claim {
        oidc["usernameClaim"] = Value::String(v.clone());
    }
    if let Some(v) = &c.username_prefix {
        oidc["usernamePrefix"] = Value::String(v.clone());
    }
    if let Some(v) = &c.groups_claim {
        oidc["groupsClaim"] = Value::String(v.clone());
    }
    if let Some(v) = &c.groups_prefix {
        oidc["groupsPrefix"] = Value::String(v.clone());
    }
    json!({ "oidc": oidc })
}

pub(crate) fn pod_identity_association_json(a: &PodIdentityAssociation) -> Value {
    let mut out = json!({
        "clusterName": a.cluster_name,
        "namespace": a.namespace,
        "serviceAccount": a.service_account,
        "roleArn": a.role_arn,
        "associationArn": a.association_arn,
        "associationId": a.association_id,
        "createdAt": timestamp_to_number(a.created_at),
        "modifiedAt": timestamp_to_number(a.modified_at),
        "disableSessionTags": a.disable_session_tags,
        "tags": a.tags,
    });
    if let Some(v) = &a.target_role_arn {
        out["targetRoleArn"] = Value::String(v.clone());
    }
    if let Some(v) = &a.external_id {
        out["externalId"] = Value::String(v.clone());
    }
    if let Some(v) = &a.owner_arn {
        out["ownerArn"] = Value::String(v.clone());
    }
    out
}

pub(crate) fn pod_identity_association_summary_json(a: &PodIdentityAssociation) -> Value {
    let mut out = json!({
        "clusterName": a.cluster_name,
        "namespace": a.namespace,
        "serviceAccount": a.service_account,
        "associationArn": a.association_arn,
        "associationId": a.association_id,
    });
    if let Some(v) = &a.owner_arn {
        out["ownerArn"] = Value::String(v.clone());
    }
    out
}

/// The real AWS EKS cluster access-policy catalogue returned by
/// `ListAccessPolicies`. Every entry is an `arn:aws:eks::aws:cluster-access-policy/*`
/// managed policy.
pub(crate) fn access_policy_catalog(region: &str) -> Vec<Value> {
    const POLICIES: &[&str] = &[
        "AmazonEKSClusterAdminPolicy",
        "AmazonEKSAdminPolicy",
        "AmazonEKSEditPolicy",
        "AmazonEKSViewPolicy",
        "AmazonEKSAdminViewPolicy",
        "AmazonEKSAutoNodePolicy",
        "AmazonEKSBlockStoragePolicy",
        "AmazonEKSLoadBalancingPolicy",
        "AmazonEKSNetworkingPolicy",
        "AmazonEKSComputePolicy",
    ];
    POLICIES
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "arn": Arn::global_in(region, "eks", "aws", &format!("cluster-access-policy/{name}")).to_string(),
            })
        })
        .collect()
}

pub(crate) fn not_found_insight(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No insight found for id: {id}."),
        )
    }
}

pub(crate) fn not_found_capability(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No capability found for name: {name}."),
        )
    }
}

pub(crate) fn not_found_subscription(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No EKS Anywhere subscription found for id: {id}."),
        )
    }
}

/// Render a connected (registered) cluster. Same shape as `cluster_json` but
/// tolerant of the empty fields a not-yet-activated connected cluster carries.
pub(crate) fn connected_cluster_json(c: &Cluster, id: &str) -> Value {
    cluster_json(c, id)
}

/// A set of plausible upgrade-readiness Insights EKS auto-generates for a
/// cluster. All seed as `PASSING` (a healthy cluster) so List/Describe return
/// real, non-empty content. Ids are generated per seed so Describe round-trips.
pub(crate) fn default_insights(cluster_version: &str) -> Vec<Insight> {
    let now = Utc::now();
    let mk = |name: &str, category: &str, description: &str, recommendation: &str| Insight {
        id: uuid::Uuid::new_v4().to_string(),
        name: name.to_string(),
        category: category.to_string(),
        kubernetes_version: cluster_version.to_string(),
        description: description.to_string(),
        recommendation: recommendation.to_string(),
        status: "PASSING".to_string(),
        reason: "No deprecated API usage detected.".to_string(),
        last_refresh_time: now,
        last_transition_time: now,
    };
    vec![
        mk(
            "Deprecated APIs removed in Kubernetes",
            "UPGRADE_READINESS",
            "Checks for usage of Kubernetes APIs that are removed in the next version.",
            "Migrate any deprecated API usage to the supported apiVersion before upgrading.",
        ),
        mk(
            "Kubelet version skew",
            "UPGRADE_READINESS",
            "Checks that node kubelet versions are within the supported skew of the control plane.",
            "Upgrade node groups so kubelet stays within two minor versions of the control plane.",
        ),
        mk(
            "EKS add-on version compatibility",
            "UPGRADE_READINESS",
            "Checks that installed EKS add-on versions are compatible with the target cluster version.",
            "Update add-ons to a version compatible with the target Kubernetes version.",
        ),
    ]
}

pub(crate) fn insight_status_json(i: &Insight) -> Value {
    json!({ "status": i.status, "reason": i.reason })
}

pub(crate) fn insight_json(i: &Insight) -> Value {
    json!({
        "id": i.id,
        "name": i.name,
        "category": i.category,
        "kubernetesVersion": i.kubernetes_version,
        "lastRefreshTime": timestamp_to_number(i.last_refresh_time),
        "lastTransitionTime": timestamp_to_number(i.last_transition_time),
        "description": i.description,
        "insightStatus": insight_status_json(i),
        "recommendation": i.recommendation,
        "additionalInfo": {},
        "resources": [],
    })
}

pub(crate) fn insight_summary_json(i: &Insight) -> Value {
    json!({
        "id": i.id,
        "name": i.name,
        "category": i.category,
        "kubernetesVersion": i.kubernetes_version,
        "lastRefreshTime": timestamp_to_number(i.last_refresh_time),
        "lastTransitionTime": timestamp_to_number(i.last_transition_time),
        "description": i.description,
        "insightStatus": insight_status_json(i),
    })
}

pub(crate) fn insights_refresh_json(r: &InsightsRefresh) -> Value {
    let mut out = json!({
        "message": "Insights refresh for the cluster.",
        "status": r.status,
        "startedAt": timestamp_to_number(r.started_at),
    });
    if let Some(ended) = r.ended_at {
        out["endedAt"] = timestamp_to_number(ended);
    }
    out
}

/// The date fakecloud's EKS version data is pinned to. AWS's version table
/// moves every few months (new minors, support tiers ending), so the cluster
/// versions below, their support status, and the add-on catalog
/// (`addon_catalog.json`, captured the same day) all describe EKS as of this
/// date. It sits in the window where Kubernetes 1.28 (extended support until
/// 2025-11-26) and 1.34 (released 2025-10-02) were both offered.
const CLUSTER_VERSIONS_AS_OF: &str = "2025-11-25";

/// One row of the `DescribeClusterVersions` catalog.
struct ClusterVersionRow {
    version: &'static str,
    /// The control plane's Kubernetes patch at the as-of date.
    patch: &'static str,
    /// The newest platform version released for the minor by the as-of date.
    platform: &'static str,
    release: &'static str,
    end_of_standard: &'static str,
    end_of_extended: &'static str,
    default: bool,
}

/// Kubernetes minors offered by EKS on `CLUSTER_VERSIONS_AS_OF`, with the
/// release and end-of-support dates from the EKS user guide's version
/// lifecycle page and the patch / platform version from its platform-versions
/// page (the 2025-11-18 platform release for every minor; 1.28's newest
/// documented platform version is eks.49).
const CLUSTER_VERSIONS: &[ClusterVersionRow] = &[
    ClusterVersionRow {
        version: "1.28",
        patch: "1.28.15",
        platform: "eks.49",
        release: "2023-09-26",
        end_of_standard: "2024-11-26",
        end_of_extended: "2025-11-26",
        default: false,
    },
    ClusterVersionRow {
        version: "1.29",
        patch: "1.29.15",
        platform: "eks.57",
        release: "2024-01-23",
        end_of_standard: "2025-03-23",
        end_of_extended: "2026-03-23",
        default: false,
    },
    ClusterVersionRow {
        version: "1.30",
        patch: "1.30.14",
        platform: "eks.54",
        release: "2024-05-23",
        end_of_standard: "2025-07-23",
        end_of_extended: "2026-07-23",
        default: false,
    },
    ClusterVersionRow {
        version: "1.31",
        patch: "1.31.13",
        platform: "eks.46",
        release: "2024-09-26",
        end_of_standard: "2025-11-26",
        end_of_extended: "2026-11-26",
        default: false,
    },
    ClusterVersionRow {
        version: "1.32",
        patch: "1.32.9",
        platform: "eks.30",
        release: "2025-01-23",
        end_of_standard: "2026-03-23",
        end_of_extended: "2027-03-23",
        default: false,
    },
    ClusterVersionRow {
        version: "1.33",
        patch: "1.33.5",
        platform: "eks.23",
        release: "2025-05-29",
        end_of_standard: "2026-07-29",
        end_of_extended: "2027-07-29",
        default: false,
    },
    ClusterVersionRow {
        version: "1.34",
        patch: "1.34.1",
        platform: "eks.9",
        release: "2025-10-02",
        end_of_standard: "2026-12-02",
        end_of_extended: "2027-12-02",
        default: true,
    },
];

/// The support tier of a version on `as_of`, as a `VersionStatus` value
/// (`STANDARD_SUPPORT`). AWS emits this same value in the deprecated
/// `status` member too, even though the model types that member as the
/// hyphenated `ClusterVersionStatus` enum; the model deprecates `status` for
/// exactly that reason ("use `versionStatus` instead, as that field matches
/// for input and output"). ISO dates compare correctly as strings.
fn support_status(row: &ClusterVersionRow, as_of: &str) -> &'static str {
    if as_of < row.end_of_standard {
        "STANDARD_SUPPORT"
    } else if as_of < row.end_of_extended {
        "EXTENDED_SUPPORT"
    } else {
        "UNSUPPORTED"
    }
}

/// The `DescribeClusterVersions` catalog: every Kubernetes minor EKS offered
/// on `CLUSTER_VERSIONS_AS_OF`, ascending, with its support status on that
/// date. 1.34 is the default (see `DEFAULT_K8S_VERSION`).
pub(crate) fn cluster_version_catalog(cluster_type: &str) -> Vec<Value> {
    CLUSTER_VERSIONS
        .iter()
        .map(|row| {
            let status = support_status(row, CLUSTER_VERSIONS_AS_OF);
            json!({
                "clusterVersion": row.version,
                "clusterType": cluster_type,
                "defaultPlatformVersion": row.platform,
                "defaultVersion": row.default,
                "releaseDate": date_to_number(row.release),
                "endOfStandardSupportDate": date_to_number(row.end_of_standard),
                "endOfExtendedSupportDate": date_to_number(row.end_of_extended),
                "status": status,
                "versionStatus": status,
                "kubernetesPatchVersion": row.patch,
            })
        })
        .collect()
}

/// Parse a `YYYY-MM-DD` date into an epoch-seconds JSON number (midnight UTC).
pub(crate) fn date_to_number(ymd: &str) -> Value {
    let ts = chrono::NaiveDate::parse_from_str(ymd, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc())
        .unwrap_or_else(Utc::now);
    timestamp_to_number(ts)
}

/// Validate an `AckConfigRequest` / `UpdateAckConfig` object against the
/// model constraints: `enableCrossNamespace` is a boolean and
/// `disabledServices` is a list of at most 200 `AckServiceName`s (1-63
/// chars, `^[a-z][a-z0-9]*$`). Violations are `InvalidParameterException`,
/// the parameter error both CreateCapability and UpdateCapability declare.
pub(crate) fn validate_ack_config(ack: &Value) -> Result<(), AwsServiceError> {
    if ack.is_null() {
        return Ok(());
    }
    let obj = ack
        .as_object()
        .ok_or_else(|| invalid_parameter("configuration.ack must be an object"))?;
    if let Some(v) = obj.get("enableCrossNamespace").filter(|v| !v.is_null()) {
        if !v.is_boolean() {
            return Err(invalid_parameter(
                "configuration.ack.enableCrossNamespace must be a boolean",
            ));
        }
    }
    if let Some(v) = obj.get("disabledServices").filter(|v| !v.is_null()) {
        let list = v.as_array().ok_or_else(|| {
            invalid_parameter("configuration.ack.disabledServices must be a list")
        })?;
        if list.len() > 200 {
            return Err(invalid_parameter(
                "configuration.ack.disabledServices must contain at most 200 service names",
            ));
        }
        for item in list {
            let name = item.as_str().unwrap_or_default();
            let valid = (1..=63).contains(&name.len())
                && name.starts_with(|c: char| c.is_ascii_lowercase())
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
            if !valid {
                return Err(invalid_parameter(format!(
                    "Invalid ACK service name '{name}' in configuration.ack.disabledServices: \
                     must be 1-63 characters matching ^[a-z][a-z0-9]*$"
                )));
            }
        }
    }
    Ok(())
}

/// Build the stored `CapabilityConfigurationResponse` for a new capability.
/// Each member is only used for its own capability type (`argoCd` for
/// ARGOCD, `ack` for ACK) and is ignored otherwise. An ACK capability always
/// reports its effective ACK settings: `enableCrossNamespace` defaults to
/// `false` when omitted (or when `ack` is omitted entirely) and
/// `disabledServices` is echoed exactly as supplied (empty when omitted, i.e.
/// every service enabled). Returns `None` when nothing applies.
pub(crate) fn normalize_capability_configuration(
    capability_type: &str,
    req: Option<&Value>,
) -> Result<Option<Value>, AwsServiceError> {
    let req = req.filter(|v| !v.is_null());
    let ack_req = req.and_then(|v| v.get("ack")).filter(|v| !v.is_null());
    if let Some(ack) = ack_req {
        validate_ack_config(ack)?;
    }
    let mut out = serde_json::Map::new();
    match capability_type {
        "ARGOCD" => {
            if let Some(argo) = req.and_then(|v| v.get("argoCd")).filter(|v| !v.is_null()) {
                out.insert("argoCd".into(), argo.clone());
            }
        }
        "ACK" => {
            let cross = ack_req
                .and_then(|a| a.get("enableCrossNamespace"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let disabled = ack_req
                .and_then(|a| a.get("disabledServices"))
                .filter(|v| v.is_array())
                .cloned()
                .unwrap_or_else(|| json!([]));
            out.insert(
                "ack".into(),
                json!({ "enableCrossNamespace": cross, "disabledServices": disabled }),
            );
        }
        _ => {}
    }
    Ok((!out.is_empty()).then_some(Value::Object(out)))
}

/// A capability's post-update configuration plus the `UpdateParam`
/// (type, value) pairs its tracked Update reports.
pub(crate) type CapabilityConfigUpdate = (Option<Value>, Vec<(String, String)>);

/// Apply an `UpdateCapabilityConfiguration` onto a stored capability
/// configuration, returning the new configuration and the `UpdateParam`s
/// the tracked Update reports. Members only apply to their own capability
/// type (`argoCd` -> ARGOCD, `ack` -> ACK) and are ignored otherwise.
///
/// `UpdateAckConfig` fields are independent: `enableCrossNamespace` (when
/// present) replaces the flag and `disabledServices` (when present)
/// replaces the whole list; each reports its own `EnableCrossNamespace` /
/// `DisabledServices` param. The ArgoCD delta reports one param per field it
/// carries: `RoleMappingsToAddOrUpdate`, `RoleMappingsToRemove`,
/// `NetworkAccess`.
pub(crate) fn apply_capability_configuration_update(
    capability_type: &str,
    existing: Option<&Value>,
    update: &Value,
) -> Result<CapabilityConfigUpdate, AwsServiceError> {
    let ack_delta = update.get("ack").filter(|v| !v.is_null());
    if let Some(ack) = ack_delta {
        validate_ack_config(ack)?;
    }
    let mut cfg = existing
        .and_then(|e| e.as_object())
        .cloned()
        .unwrap_or_default();
    let mut params = Vec::new();
    match capability_type {
        "ARGOCD" => {
            if let Some(delta) = update.get("argoCd").filter(|v| !v.is_null()) {
                let argo = apply_argocd_update(cfg.get("argoCd"), delta);
                cfg.insert("argoCd".into(), argo);
                let rbac = delta.get("rbacRoleMappings");
                for (param, value) in [
                    (
                        "RoleMappingsToAddOrUpdate",
                        rbac.and_then(|r| r.get("addOrUpdateRoleMappings")),
                    ),
                    (
                        "RoleMappingsToRemove",
                        rbac.and_then(|r| r.get("removeRoleMappings")),
                    ),
                    ("NetworkAccess", delta.get("networkAccess")),
                ] {
                    if let Some(v) = value.filter(|v| !v.is_null()) {
                        params.push((param.to_string(), v.to_string()));
                    }
                }
            }
        }
        "ACK" => {
            let mut ack = cfg
                .get("ack")
                .and_then(|a| a.as_object())
                .cloned()
                .unwrap_or_default();
            ack.entry("enableCrossNamespace")
                .or_insert(Value::Bool(false));
            ack.entry("disabledServices").or_insert_with(|| json!([]));
            if let Some(delta) = ack_delta {
                if let Some(cross) = delta.get("enableCrossNamespace").and_then(Value::as_bool) {
                    ack.insert("enableCrossNamespace".into(), Value::Bool(cross));
                    params.push(("EnableCrossNamespace".to_string(), cross.to_string()));
                }
                if let Some(list) = delta.get("disabledServices").filter(|v| v.is_array()) {
                    ack.insert("disabledServices".into(), list.clone());
                    params.push(("DisabledServices".to_string(), list.to_string()));
                }
            }
            cfg.insert("ack".into(), Value::Object(ack));
        }
        _ => {}
    }
    Ok(((!cfg.is_empty()).then_some(Value::Object(cfg)), params))
}

/// Apply an `UpdateArgoCdConfig` delta onto a stored `argoCd` configuration.
/// `UpdateArgoCdConfig` only carries `networkAccess` (which replaces the
/// stored one) and `rbacRoleMappings` add/remove deltas; every other
/// create-time member (`namespace`, `awsIdc`, `endpointPrefix`, which can't
/// change after creation) is preserved.
fn apply_argocd_update(existing: Option<&Value>, delta: &Value) -> Value {
    let mut argo = existing
        .and_then(|a| a.as_object())
        .cloned()
        .unwrap_or_default();
    if let Some(net) = delta.get("networkAccess") {
        argo.insert("networkAccess".into(), net.clone());
    }
    if let Some(rbac) = delta.get("rbacRoleMappings") {
        let mut mappings: Vec<Value> = argo
            .get("rbacRoleMappings")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let identities_of = |m: &Value| -> Vec<Value> {
            m.get("identities")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        };
        let same_identity =
            |a: &Value, b: &Value| a.get("id") == b.get("id") && a.get("type") == b.get("type");
        for add in rbac
            .get("addOrUpdateRoleMappings")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let role = add.get("role");
            match mappings.iter_mut().find(|m| m.get("role") == role) {
                Some(existing) => {
                    let mut ids = identities_of(existing);
                    for id in identities_of(add) {
                        if !ids.iter().any(|e| same_identity(e, &id)) {
                            ids.push(id);
                        }
                    }
                    existing["identities"] = Value::Array(ids);
                }
                None => mappings.push(add.clone()),
            }
        }
        for remove in rbac
            .get("removeRoleMappings")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let role = remove.get("role");
            let gone = identities_of(remove);
            for m in mappings.iter_mut().filter(|m| m.get("role") == role) {
                let kept: Vec<Value> = identities_of(m)
                    .into_iter()
                    .filter(|id| !gone.iter().any(|g| same_identity(g, id)))
                    .collect();
                m["identities"] = Value::Array(kept);
            }
            mappings.retain(|m| !identities_of(m).is_empty());
        }
        argo.insert("rbacRoleMappings".into(), Value::Array(mappings));
    }
    Value::Object(argo)
}

pub(crate) fn capability_json(c: &Capability) -> Value {
    let mut out = json!({
        "capabilityName": c.name,
        "arn": c.arn,
        "clusterName": c.cluster_name,
        "type": c.type_,
        "roleArn": c.role_arn,
        "status": c.status,
        "version": c.version,
        "tags": c.tags,
        "health": { "issues": [] },
        "createdAt": timestamp_to_number(c.created_at),
        "modifiedAt": timestamp_to_number(c.modified_at),
    });
    if let Some(cfg) = &c.configuration {
        out["configuration"] = cfg.clone();
    }
    // An ACK capability always reports its effective settings, including one
    // persisted before the `ack` block existed.
    if c.type_ == "ACK" && out["configuration"].get("ack").is_none() {
        out["configuration"]["ack"] = json!({
            "enableCrossNamespace": false,
            "disabledServices": [],
        });
    }
    if let Some(p) = &c.delete_propagation_policy {
        out["deletePropagationPolicy"] = Value::String(p.clone());
    }
    out
}

pub(crate) fn capability_summary_json(c: &Capability) -> Value {
    json!({
        "capabilityName": c.name,
        "arn": c.arn,
        "type": c.type_,
        "status": c.status,
        "version": c.version,
        "createdAt": timestamp_to_number(c.created_at),
        "modifiedAt": timestamp_to_number(c.modified_at),
    })
}

pub(crate) fn subscription_json(s: &EksAnywhereSubscription) -> Value {
    json!({
        "id": s.id,
        "arn": s.arn,
        "createdAt": timestamp_to_number(s.created_at),
        "effectiveDate": timestamp_to_number(s.effective_date),
        "expirationDate": timestamp_to_number(s.expiration_date),
        "licenseQuantity": s.license_quantity,
        "licenseType": s.license_type,
        "term": { "duration": s.term_duration, "unit": s.term_unit },
        "status": s.status,
        "autoRenew": s.auto_renew,
        "licenseArns": [],
        "licenses": [],
        "tags": s.tags,
    })
}

pub(crate) fn not_found_certificate_authority(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No certificate authority found for id: {id}."),
        )
    }
}

/// Build a fresh CA record for `cluster`, valid for one year from now.
/// `created_by` is `EKS` for the CA seeded with the cluster and `CUSTOMER`
/// for one added through `CreateCertificateAuthority`.
pub(crate) fn new_certificate_authority(cluster: &str, created_by: &str) -> CertificateAuthority {
    let now = Utc::now();
    CertificateAuthority {
        id: uuid::Uuid::new_v4().to_string(),
        cluster_name: cluster.to_string(),
        created_by: created_by.to_string(),
        created_at: now,
        activated_at: None,
        activated_by: None,
        signing_status: "NOT_USED".to_string(),
        distribution_status: "IN_PROGRESS".to_string(),
        not_before: now,
        not_after: now + chrono::Duration::days(365),
        rollback_available: false,
        data: default_ca_data(),
    }
}

/// The `CertificateAuthoritySummary` members shared by both response shapes.
fn certificate_authority_summary_members(
    ca: &CertificateAuthority,
) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    out.insert("id".to_string(), Value::String(ca.id.clone()));
    out.insert("createdAt".to_string(), timestamp_to_number(ca.created_at));
    out.insert(
        "createdBy".to_string(),
        Value::String(ca.created_by.clone()),
    );
    if let Some(at) = ca.activated_at {
        out.insert("activatedAt".to_string(), timestamp_to_number(at));
    }
    if let Some(by) = &ca.activated_by {
        out.insert("activatedBy".to_string(), Value::String(by.clone()));
    }
    out.insert(
        "signingStatus".to_string(),
        Value::String(ca.signing_status.clone()),
    );
    out.insert(
        "distributionStatus".to_string(),
        Value::String(ca.distribution_status.clone()),
    );
    out
}

pub(crate) fn certificate_authority_summary_json(ca: &CertificateAuthority) -> Value {
    Value::Object(certificate_authority_summary_members(ca))
}

pub(crate) fn certificate_authority_json(ca: &CertificateAuthority) -> Value {
    let mut out = certificate_authority_summary_members(ca);
    out.insert(
        "validity".to_string(),
        json!({
            "notBefore": timestamp_to_number(ca.not_before),
            "notAfter": timestamp_to_number(ca.not_after),
        }),
    );
    out.insert(
        "rollbackAvailable".to_string(),
        Value::Bool(ca.rollback_available),
    );
    out.insert("data".to_string(), Value::String(ca.data.clone()));
    Value::Object(out)
}

#[cfg(test)]
mod capability_json_tests {
    use super::*;

    #[test]
    fn ack_capability_without_stored_config_reports_defaults() {
        // A capability persisted before the `ack` block existed has no
        // configuration; it still reports the effective ACK settings.
        let now = Utc::now();
        let c = Capability {
            name: "ack".into(),
            arn: "arn:aws:eks:us-east-1:123456789012:capability/c1/ack/x".into(),
            cluster_name: "c1".into(),
            type_: "ACK".into(),
            role_arn: "arn:aws:iam::123456789012:role/r".into(),
            status: "ACTIVE".into(),
            version: "1".into(),
            configuration: None,
            tags: Default::default(),
            created_at: now,
            modified_at: now,
            delete_propagation_policy: None,
        };
        assert_eq!(
            capability_json(&c)["configuration"],
            json!({"ack": {"enableCrossNamespace": false, "disabledServices": []}})
        );
        let kro = Capability {
            type_: "KRO".into(),
            ..c
        };
        assert!(capability_json(&kro).get("configuration").is_none());
    }
}
