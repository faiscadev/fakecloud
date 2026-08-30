//! Launch templates (+ versions), Spot instance/fleet requests, EC2 fleets,
//! and the spot datafeed subscription.

use std::collections::BTreeMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, require, require_struct, validate_enum, validate_int_range,
    validate_length, validate_max_results,
};
use crate::state::{Ec2State, Fleet, LaunchTemplate, SpotFleet, SpotRequest, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

/// Shared `LaunchTemplateName` (3..128) + `VersionDescription` (0..255) checks.
fn validate_lt_strings(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_length(&req.query_params, "LaunchTemplateName", 3, 128)?;
    validate_length(&req.query_params, "VersionDescription", 0, 255)?;
    Ok(())
}

// ---- launch templates ----

fn lt_xml(t: &LaunchTemplate, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}<defaultVersionNumber>{}</defaultVersionNumber><latestVersionNumber>{}</latestVersionNumber>{}",
        ec2_elem("launchTemplateId", &t.id),
        ec2_elem("launchTemplateName", &t.name),
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem("createdBy", &format!("arn:aws:iam::{owner}:root")),
        t.default_version,
        t.latest_version,
        super::tags::tag_set_xml(tags),
    )
}

/// Extract the flattened `LaunchTemplateData.*` sub-map from a request: every
/// query key under the `LaunchTemplateData.` prefix, with that prefix stripped.
/// Stored verbatim so the whole structure round-trips (nothing is discarded on
/// the write side); `render_lt_data` projects the wire shape back on read.
fn collect_lt_data(params: &std::collections::HashMap<String, String>) -> BTreeMap<String, String> {
    params
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("LaunchTemplateData.")
                .map(|suffix| (suffix.to_string(), v.clone()))
        })
        .collect()
}

/// Collect a flattened EC2 list (`<prefix>.1`, `<prefix>.2`, …) from the stored
/// data sub-map, in index order, stopping at the first gap.
fn lt_indexed(d: &BTreeMap<String, String>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1;
    while let Some(v) = d.get(&format!("{prefix}.{i}")) {
        out.push(v.clone());
        i += 1;
    }
    out
}

/// Render one nested struct member if any of its sub-keys are present, wrapping
/// the joined inner elements in `<wrapper>…</wrapper>`. Returns empty when the
/// struct has no set members (EC2 omits absent optional structs).
fn lt_struct(wrapper: &str, inner: String) -> String {
    if inner.is_empty() {
        String::new()
    } else {
        format!("<{wrapper}>{inner}</{wrapper}>")
    }
}

/// Project a stored `LaunchTemplateData` sub-map back to the DescribeLaunch\
/// TemplateVersions `<launchTemplateData>` wire shape. Scalars, id/name lists,
/// and the common nested structs are emitted with their exact EC2 response
/// element names; every stored key is preserved even if a given member is not
/// re-rendered, so no written value is lost.
fn render_lt_data(d: &BTreeMap<String, String>) -> String {
    if d.is_empty() {
        return "<launchTemplateData/>".to_string();
    }
    let mut out = String::from("<launchTemplateData>");

    // Scalar members: request-suffix -> response element name.
    for (suffix, elem) in [
        ("ImageId", "imageId"),
        ("InstanceType", "instanceType"),
        ("KeyName", "keyName"),
        ("UserData", "userData"),
        ("EbsOptimized", "ebsOptimized"),
        ("DisableApiTermination", "disableApiTermination"),
        ("DisableApiStop", "disableApiStop"),
        (
            "InstanceInitiatedShutdownBehavior",
            "instanceInitiatedShutdownBehavior",
        ),
        ("KernelId", "kernelId"),
        ("RamDiskId", "ramDiskId"),
    ] {
        if let Some(v) = d.get(suffix) {
            out.push_str(&ec2_elem(elem, v));
        }
    }

    // Security groups (by id and by name).
    out.push_str(&ec2_list(
        "securityGroupIdSet",
        &lt_indexed(d, "SecurityGroupId"),
    ));
    out.push_str(&ec2_list(
        "securityGroupSet",
        &lt_indexed(d, "SecurityGroupName"),
    ));

    // Monitoring.
    if let Some(v) = d.get("Monitoring.Enabled") {
        out.push_str(&lt_struct("monitoring", ec2_elem("enabled", v)));
    }
    // IAM instance profile.
    let iam = format!(
        "{}{}",
        d.get("IamInstanceProfile.Arn")
            .map(|v| ec2_elem("arn", v))
            .unwrap_or_default(),
        d.get("IamInstanceProfile.Name")
            .map(|v| ec2_elem("name", v))
            .unwrap_or_default(),
    );
    out.push_str(&lt_struct("iamInstanceProfile", iam));
    // Placement.
    let placement = [
        ("Placement.AvailabilityZone", "availabilityZone"),
        ("Placement.GroupName", "groupName"),
        ("Placement.Tenancy", "tenancy"),
        ("Placement.Affinity", "affinity"),
        ("Placement.HostId", "hostId"),
        ("Placement.PartitionNumber", "partitionNumber"),
        ("Placement.HostResourceGroupArn", "hostResourceGroupArn"),
    ]
    .iter()
    .filter_map(|(k, e)| d.get(*k).map(|v| ec2_elem(e, v)))
    .collect::<String>();
    out.push_str(&lt_struct("placement", placement));
    // CPU options.
    let cpu = [
        ("CpuOptions.CoreCount", "coreCount"),
        ("CpuOptions.ThreadsPerCore", "threadsPerCore"),
        ("CpuOptions.AmdSevSnp", "amdSevSnp"),
    ]
    .iter()
    .filter_map(|(k, e)| d.get(*k).map(|v| ec2_elem(e, v)))
    .collect::<String>();
    out.push_str(&lt_struct("cpuOptions", cpu));
    // Metadata options.
    let meta = [
        ("MetadataOptions.HttpTokens", "httpTokens"),
        (
            "MetadataOptions.HttpPutResponseHopLimit",
            "httpPutResponseHopLimit",
        ),
        ("MetadataOptions.HttpEndpoint", "httpEndpoint"),
        ("MetadataOptions.HttpProtocolIpv6", "httpProtocolIpv6"),
        (
            "MetadataOptions.InstanceMetadataTags",
            "instanceMetadataTags",
        ),
    ]
    .iter()
    .filter_map(|(k, e)| d.get(*k).map(|v| ec2_elem(e, v)))
    .collect::<String>();
    out.push_str(&lt_struct("metadataOptions", meta));
    // Credit specification.
    if let Some(v) = d.get("CreditSpecification.CpuCredits") {
        out.push_str(&lt_struct("creditSpecification", ec2_elem("cpuCredits", v)));
    }

    // Block device mappings (indexed list of structs).
    let mut bdms: Vec<String> = Vec::new();
    let mut i = 1;
    while d.contains_key(&format!("BlockDeviceMapping.{i}.DeviceName"))
        || d.contains_key(&format!("BlockDeviceMapping.{i}.Ebs.VolumeSize"))
        || d.contains_key(&format!("BlockDeviceMapping.{i}.VirtualName"))
        || d.contains_key(&format!("BlockDeviceMapping.{i}.NoDevice"))
    {
        let p = format!("BlockDeviceMapping.{i}");
        let ebs = [
            ("Ebs.VolumeSize", "volumeSize"),
            ("Ebs.VolumeType", "volumeType"),
            ("Ebs.Iops", "iops"),
            ("Ebs.Throughput", "throughput"),
            ("Ebs.DeleteOnTermination", "deleteOnTermination"),
            ("Ebs.Encrypted", "encrypted"),
            ("Ebs.SnapshotId", "snapshotId"),
            ("Ebs.KmsKeyId", "kmsKeyId"),
        ]
        .iter()
        .filter_map(|(k, e)| d.get(&format!("{p}.{k}")).map(|v| ec2_elem(e, v)))
        .collect::<String>();
        let item = format!(
            "{}{}{}{}",
            d.get(&format!("{p}.DeviceName"))
                .map(|v| ec2_elem("deviceName", v))
                .unwrap_or_default(),
            d.get(&format!("{p}.VirtualName"))
                .map(|v| ec2_elem("virtualName", v))
                .unwrap_or_default(),
            d.get(&format!("{p}.NoDevice"))
                .map(|v| ec2_elem("noDevice", v))
                .unwrap_or_default(),
            lt_struct("ebs", ebs),
        );
        bdms.push(item);
        i += 1;
    }
    out.push_str(&ec2_list("blockDeviceMappingSet", &bdms));

    // Tag specifications (indexed list; each carries a resource type + tag set).
    let mut tag_specs: Vec<String> = Vec::new();
    let mut i = 1;
    while d.contains_key(&format!("TagSpecification.{i}.ResourceType"))
        || d.contains_key(&format!("TagSpecification.{i}.Tag.1.Key"))
    {
        let p = format!("TagSpecification.{i}");
        let mut tags: Vec<String> = Vec::new();
        let mut j = 1;
        while let Some(k) = d.get(&format!("{p}.Tag.{j}.Key")) {
            let val = d
                .get(&format!("{p}.Tag.{j}.Value"))
                .cloned()
                .unwrap_or_default();
            tags.push(format!("{}{}", ec2_elem("key", k), ec2_elem("value", &val)));
            j += 1;
        }
        let item = format!(
            "{}{}",
            d.get(&format!("{p}.ResourceType"))
                .map(|v| ec2_elem("resourceType", v))
                .unwrap_or_default(),
            ec2_list("tagSet", &tags),
        );
        tag_specs.push(item);
        i += 1;
    }
    out.push_str(&ec2_list("tagSpecificationSet", &tag_specs));

    out.push_str("</launchTemplateData>");
    out
}

fn lt_version_xml(
    t: &LaunchTemplate,
    version: i64,
    owner: &str,
    data: &BTreeMap<String, String>,
) -> String {
    format!(
        "{}{}<versionNumber>{}</versionNumber>{}{}<defaultVersion>{}</defaultVersion>{}",
        ec2_elem("launchTemplateId", &t.id),
        ec2_elem("launchTemplateName", &t.name),
        version,
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem("createdBy", &format!("arn:aws:iam::{owner}:root")),
        version == t.default_version,
        render_lt_data(data),
    )
}

pub(crate) fn create_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // CreateLaunchTemplate's LaunchTemplateName is unconstrained (unlike the
    // other launch-template ops); only VersionDescription is length-bounded.
    // LaunchTemplateData is a required struct with no required members, so an
    // empty one is wire-invisible and can't be enforced (see require_struct).
    validate_length(&req.query_params, "VersionDescription", 0, 255)?;
    let name = require(&req.query_params, "LaunchTemplateName")?;
    let id = gen_id("lt");
    let t = LaunchTemplate {
        id: id.clone(),
        name,
        default_version: 1,
        latest_version: 1,
        versions: BTreeMap::from([(1, collect_lt_data(&req.query_params))]),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "launch-template",
        );
        let tg = state.tags_for(&id).to_vec();
        state.launch_templates.insert(id.clone(), t.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateLaunchTemplate",
        &req.request_id,
        &format!(
            "<launchTemplate>{}</launchTemplate>",
            lt_xml(&t, &tags, &owner)
        ),
    ))
}

pub(crate) fn create_launch_template_version(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    // LaunchTemplateData is a required struct, but RequestLaunchTemplateData has
    // no required members, so an empty one is wire-invisible (indistinguishable
    // from omission) and cannot be enforced here — see require_struct docs.
    let owner = req.account_id.clone();
    let id = req.query_params.get("LaunchTemplateId").cloned();
    let name = req.query_params.get("LaunchTemplateName").cloned();
    let data = collect_lt_data(&req.query_params);
    let (t, version) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = id
            .clone()
            .or_else(|| {
                name.as_ref().and_then(|n| {
                    state
                        .launch_templates
                        .values()
                        .find(|t| &t.name == n)
                        .map(|t| t.id.clone())
                })
            })
            .unwrap_or_default();
        if let Some(t) = state.launch_templates.get_mut(&key) {
            t.latest_version += 1;
            let v = t.latest_version;
            t.versions.insert(v, data.clone());
            (t.clone(), v)
        } else {
            // Unknown template: synthesize a response-only record (do NOT
            // persist — fabricating a template for a version request on a
            // non-existent template would leave bogus state behind).
            let synthetic = LaunchTemplate {
                id: id.unwrap_or_else(|| gen_id("lt")),
                name: name.unwrap_or_default(),
                default_version: 1,
                latest_version: 2,
                versions: BTreeMap::from([(2, data.clone())]),
            };
            (synthetic, 2)
        }
    };
    Ok(Ec2Service::respond(
        "CreateLaunchTemplateVersion",
        &req.request_id,
        &format!(
            "<launchTemplateVersion>{}</launchTemplateVersion>",
            lt_version_xml(&t, version, &owner, &data)
        ),
    ))
}

fn resolve_lt(state: &Ec2State, req: &AwsRequest) -> Option<LaunchTemplate> {
    if let Some(id) = req.query_params.get("LaunchTemplateId") {
        return state.launch_templates.get(id).cloned();
    }
    if let Some(name) = req.query_params.get("LaunchTemplateName") {
        return state
            .launch_templates
            .values()
            .find(|t| &t.name == name)
            .cloned();
    }
    None
}

pub(crate) fn delete_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let t = resolve_lt(state, req);
    let tags = t
        .as_ref()
        .map(|t| state.tags_for(&t.id).to_vec())
        .unwrap_or_default();
    let body = if let Some(t) = t {
        state.launch_templates.remove(&t.id);
        state.tags.remove(&t.id);
        format!(
            "<launchTemplate>{}</launchTemplate>",
            lt_xml(&t, &tags, &owner)
        )
    } else {
        String::new()
    };
    Ok(Ec2Service::respond(
        "DeleteLaunchTemplate",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_launch_template_versions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let id = req
        .query_params
        .get("LaunchTemplateId")
        .cloned()
        .unwrap_or_default();
    let versions = indexed_list(&req.query_params, "LaunchTemplateVersion");
    let items: Vec<String> = versions
        .iter()
        .map(|v| {
            format!(
                "{}<versionNumber>{}</versionNumber>",
                ec2_elem("launchTemplateId", &id),
                v
            )
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successfullyDeletedLaunchTemplateVersionSet", &items),
        ec2_list("unsuccessfullyDeletedLaunchTemplateVersionSet", &[])
    );
    Ok(Ec2Service::respond(
        "DeleteLaunchTemplateVersions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_launch_templates(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 200)?;
    let wanted = indexed_list(&req.query_params, "LaunchTemplateId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .launch_templates
        .values()
        .filter(|t| wanted.is_empty() || wanted.contains(&t.id))
        .map(|t| lt_xml(t, state.tags_for(&t.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLaunchTemplates",
        &req.request_id,
        &ec2_list("launchTemplates", &items),
    ))
}

pub(crate) fn describe_launch_template_versions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let empty_data = BTreeMap::new();
    let items: Vec<String> = resolve_lt(state, req)
        .map(|t| {
            (1..=t.latest_version)
                .map(|v| {
                    let data = t.versions.get(&v).unwrap_or(&empty_data);
                    lt_version_xml(&t, v, &owner, data)
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "DescribeLaunchTemplateVersions",
        &req.request_id,
        &ec2_list("launchTemplateVersionSet", &items),
    ))
}

pub(crate) fn get_launch_template_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "GetLaunchTemplateData",
        &req.request_id,
        "<launchTemplateData><instanceType>t3.micro</instanceType></launchTemplateData>",
    ))
}

pub(crate) fn modify_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if let (Some(t), Some(v)) = (
        resolve_lt(state, req),
        req.query_params
            .get("DefaultVersion")
            .and_then(|v| v.parse::<i64>().ok()),
    ) {
        if let Some(t) = state.launch_templates.get_mut(&t.id) {
            t.default_version = v;
        }
    }
    let t = resolve_lt(state, req);
    let tags = t
        .as_ref()
        .map(|t| state.tags_for(&t.id).to_vec())
        .unwrap_or_default();
    let body = t
        .map(|t| {
            format!(
                "<launchTemplate>{}</launchTemplate>",
                lt_xml(&t, &tags, &owner)
            )
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyLaunchTemplate",
        &req.request_id,
        &body,
    ))
}

// ---- spot instance requests ----

fn spot_request_xml(r: &SpotRequest, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}<status><code>{}</code><message>request fulfilled</message></status>{}{}{}",
        ec2_elem("spotInstanceRequestId", &r.id),
        ec2_elem("state", &r.state),
        ec2_elem("type", &r.request_type),
        ec2_elem("spotPrice", &r.spot_price),
        r.state,
        ec2_elem("productDescription", "Linux/UNIX"),
        ec2_elem("createTime", FIXED_TIME),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn request_spot_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "InstanceInterruptionBehavior",
        &["hibernate", "stop", "terminate"],
    )?;
    validate_enum(&req.query_params, "Type", &["one-time", "persistent"])?;
    let count: usize = req
        .query_params
        .get("InstanceCount")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let price = req
        .query_params
        .get("SpotPrice")
        .cloned()
        .unwrap_or_else(|| "0.05".to_string());
    let rtype = req
        .query_params
        .get("Type")
        .cloned()
        .unwrap_or_else(|| "one-time".to_string());
    let mut rendered = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for _ in 0..count.max(1) {
            let id = gen_id("sir");
            let r = SpotRequest {
                id: id.clone(),
                state: "active".to_string(),
                request_type: rtype.clone(),
                spot_price: price.clone(),
            };
            rendered.push(spot_request_xml(&r, &[]));
            state.spot_requests.insert(id, r);
        }
    }
    Ok(Ec2Service::respond(
        "RequestSpotInstances",
        &req.request_id,
        &ec2_list("spotInstanceRequestSet", &rendered),
    ))
}

pub(crate) fn describe_spot_instance_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "SpotInstanceRequestId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .spot_requests
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| spot_request_xml(r, state.tags_for(&r.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeSpotInstanceRequests",
        &req.request_id,
        &ec2_list("spotInstanceRequestSet", &items),
    ))
}

pub(crate) fn cancel_spot_instance_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "SpotInstanceRequestId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            if let Some(r) = state.spot_requests.get_mut(id) {
                r.state = "cancelled".to_string();
            }
        }
    }
    let items: Vec<String> = ids
        .iter()
        .map(|id| {
            format!(
                "{}{}",
                ec2_elem("spotInstanceRequestId", id),
                ec2_elem("state", "cancelled")
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "CancelSpotInstanceRequests",
        &req.request_id,
        &ec2_list("spotInstanceRequestSet", &items),
    ))
}

// ---- spot fleet ----

pub(crate) fn request_spot_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "SpotFleetRequestConfig")?;
    let id = gen_id("sfr");
    let target_capacity = req
        .query_params
        .get("SpotFleetRequestConfig.TargetCapacity")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).spot_fleets.insert(
            id.clone(),
            SpotFleet {
                id: id.clone(),
                state: "active".to_string(),
                target_capacity,
            },
        );
    }
    Ok(Ec2Service::respond(
        "RequestSpotFleet",
        &req.request_id,
        &ec2_elem("spotFleetRequestId", &id),
    ))
}

pub(crate) fn describe_spot_fleet_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "SpotFleetRequestId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .spot_fleets
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| {
            format!(
                "{}{}<spotFleetRequestConfig><targetCapacity>{}</targetCapacity></spotFleetRequestConfig>{}",
                ec2_elem("spotFleetRequestId", &f.id),
                ec2_elem("spotFleetRequestState", &f.state),
                f.target_capacity,
                ec2_elem("createTime", FIXED_TIME),
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeSpotFleetRequests",
        &req.request_id,
        &ec2_list("spotFleetRequestConfigSet", &items),
    ))
}

pub(crate) fn cancel_spot_fleet_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let terminate = require(&req.query_params, "TerminateInstances")? == "true";
    // TerminateInstances drives the resulting state: terminating tears down the
    // running instances, otherwise the fleet keeps them but stops replacing.
    let new_state = if terminate {
        "cancelled_terminating"
    } else {
        "cancelled_running"
    };
    let ids = indexed_list(&req.query_params, "SpotFleetRequestId");
    let mut items = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            let prev = state
                .spot_fleets
                .get(id)
                .map(|f| f.state.clone())
                .unwrap_or_else(|| "active".to_string());
            if let Some(f) = state.spot_fleets.get_mut(id) {
                f.state = new_state.to_string();
            }
            items.push(format!(
                "{}<currentSpotFleetRequestState>{new_state}</currentSpotFleetRequestState><previousSpotFleetRequestState>{prev}</previousSpotFleetRequestState>",
                ec2_elem("spotFleetRequestId", id)
            ));
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successfulFleetRequestSet", &items),
        ec2_list("unsuccessfulFleetRequestSet", &[])
    );
    Ok(Ec2Service::respond(
        "CancelSpotFleetRequests",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_spot_fleet_request(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SpotFleetRequestId")?;
    validate_enum(
        &req.query_params,
        "ExcessCapacityTerminationPolicy",
        &["noTermination", "default"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let fleet = state.spot_fleets.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidSpotFleetRequestId.NotFound",
                format!("The spot fleet request ID '{id}' does not exist"),
            )
        })?;
        if let Some(tc) = req
            .query_params
            .get("TargetCapacity")
            .and_then(|v| v.parse::<i64>().ok())
        {
            fleet.target_capacity = tc;
        }
    }
    Ok(Ec2Service::respond(
        "ModifySpotFleetRequest",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_spot_fleet_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SpotFleetRequestId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let body = format!(
        "{}{}",
        ec2_elem("spotFleetRequestId", &id),
        ec2_list("activeInstanceSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeSpotFleetInstances",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_spot_fleet_request_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SpotFleetRequestId")?;
    let start = require(&req.query_params, "StartTime")?;
    validate_enum(
        &req.query_params,
        "EventType",
        &[
            "instanceChange",
            "fleetRequestChange",
            "error",
            "information",
        ],
    )?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let body = format!(
        "{}{}{}{}",
        ec2_elem("spotFleetRequestId", &id),
        ec2_elem("startTime", &start),
        ec2_elem("lastEvaluatedTime", FIXED_TIME),
        ec2_list("historyRecordSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeSpotFleetRequestHistory",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_spot_price_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let az = req
        .query_params
        .get("AvailabilityZone")
        .cloned()
        .unwrap_or_else(|| {
            format!(
                "{}a",
                if req.region.is_empty() {
                    "us-east-1"
                } else {
                    &req.region
                }
            )
        });
    let item = format!(
        "{}{}{}{}{}",
        ec2_elem("instanceType", "t3.micro"),
        ec2_elem("productDescription", "Linux/UNIX"),
        ec2_elem("spotPrice", "0.0035"),
        ec2_elem("timestamp", FIXED_TIME),
        ec2_elem("availabilityZone", &az),
    );
    Ok(Ec2Service::respond(
        "DescribeSpotPriceHistory",
        &req.request_id,
        &ec2_list("spotPriceHistorySet", &[item]),
    ))
}

pub(crate) fn get_spot_placement_scores(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TargetCapacity")?;
    validate_int_range(&req.query_params, "TargetCapacity", 1, 2_000_000_000)?;
    validate_enum(
        &req.query_params,
        "TargetCapacityUnitType",
        &["vcpu", "memory-mib", "units"],
    )?;
    validate_max_results(&req.query_params, 10, 1000)?;
    // `IncludeLocalZones` widens the scored set to Local Zones. fakecloud
    // models only the three standard availability zones per region (see
    // `DescribeAvailabilityZones`, which reports `zoneType`
    // `availability-zone` for all of them), so there is no Local Zone capacity
    // to score and the result set is the same either way — but the flag is
    // still a boolean and a non-boolean value is rejected as AWS would.
    validate_enum(&req.query_params, "IncludeLocalZones", &["true", "false"])?;
    let region = if req.region.is_empty() {
        "us-east-1"
    } else {
        &req.region
    };
    let item = format!("{}<score>9</score>", ec2_elem("region", region));
    Ok(Ec2Service::respond(
        "GetSpotPlacementScores",
        &req.request_id,
        &ec2_list("spotPlacementScoreSet", &[item]),
    ))
}

// ---- spot datafeed subscription ----

fn datafeed_xml(bucket: &str, prefix: &str, owner: &str) -> String {
    format!(
        "{}{}{}<state>Active</state>",
        ec2_elem("ownerId", owner),
        ec2_elem("bucket", bucket),
        ec2_elem("prefix", prefix),
    )
}

pub(crate) fn create_spot_datafeed_subscription(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let bucket = require(&req.query_params, "Bucket")?;
    let prefix = req.query_params.get("Prefix").cloned().unwrap_or_default();
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).spot_datafeed =
            Some((bucket.clone(), prefix.clone()));
    }
    Ok(Ec2Service::respond(
        "CreateSpotDatafeedSubscription",
        &req.request_id,
        &format!(
            "<spotDatafeedSubscription>{}</spotDatafeedSubscription>",
            datafeed_xml(&bucket, &prefix, &req.account_id)
        ),
    ))
}

pub(crate) fn delete_spot_datafeed_subscription(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).spot_datafeed = None;
    }
    Ok(Ec2Service::respond(
        "DeleteSpotDatafeedSubscription",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_spot_datafeed_subscription(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let sub = accounts
        .get(&req.account_id)
        .and_then(|s| s.spot_datafeed.clone());
    // Only emit the subscription element when one actually exists; don't
    // fabricate a phantom subscription for an account that never created one.
    let body = match sub {
        Some((bucket, prefix)) => format!(
            "<spotDatafeedSubscription>{}</spotDatafeedSubscription>",
            datafeed_xml(&bucket, &prefix, &owner)
        ),
        None => String::new(),
    };
    Ok(Ec2Service::respond(
        "DescribeSpotDatafeedSubscription",
        &req.request_id,
        &body,
    ))
}

// ---- EC2 fleets ----

pub(crate) fn create_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "TargetCapacitySpecification")?;
    validate_enum(
        &req.query_params,
        "ExcessCapacityTerminationPolicy",
        &["no-termination", "termination"],
    )?;
    validate_enum(
        &req.query_params,
        "Type",
        &["request", "maintain", "instant"],
    )?;
    let id = gen_id("fleet");
    let ftype = req
        .query_params
        .get("Type")
        .cloned()
        .unwrap_or_else(|| "maintain".to_string());
    let target_capacity = req
        .query_params
        .get("TargetCapacitySpecification.TotalTargetCapacity")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).fleets.insert(
            id.clone(),
            Fleet {
                id: id.clone(),
                state: "active".to_string(),
                fleet_type: ftype,
                target_capacity,
            },
        );
    }
    let body = format!(
        "{}{}{}",
        ec2_elem("fleetId", &id),
        ec2_list("errorSet", &[]),
        ec2_list("fleetInstanceSet", &[])
    );
    Ok(Ec2Service::respond("CreateFleet", &req.request_id, &body))
}

/// Render an `UnsuccessfulFleetDeletionItem` (fleetId + error code/message).
fn delete_fleet_error(id: &str, code: &str, message: &str) -> String {
    format!(
        "{}<error><code>{code}</code><message>{message}</message></error>",
        ec2_elem("fleetId", id)
    )
}

pub(crate) fn delete_fleets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let terminate = require(&req.query_params, "TerminateInstances")? == "true";
    let new_state = if terminate {
        "deleted_terminating"
    } else {
        "deleted_running"
    };
    let ids = indexed_list(&req.query_params, "FleetId");
    let mut successful = Vec::new();
    let mut unsuccessful = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            let Some(fleet) = state.fleets.get(id).cloned() else {
                // Unknown fleet id -> unsuccessful, not a phantom success.
                unsuccessful.push(delete_fleet_error(
                    id,
                    "fleetIdDoesNotExist",
                    "The fleet ID does not exist",
                ));
                continue;
            };
            // A non-terminating delete is invalid for `instant` fleets, which
            // have no ongoing capacity to keep running.
            if fleet.fleet_type == "instant" && !terminate {
                unsuccessful.push(delete_fleet_error(
                    id,
                    "fleetNotInModifiableState",
                    "instant fleets must be deleted with TerminateInstances",
                ));
                continue;
            }
            // AWS keeps deleted fleets visible in a deleted_* state rather than
            // dropping them immediately, so transition instead of removing.
            if let Some(f) = state.fleets.get_mut(id) {
                f.state = new_state.to_string();
            }
            successful.push(format!(
                "{}<currentFleetState>{new_state}</currentFleetState><previousFleetState>{}</previousFleetState>",
                ec2_elem("fleetId", id),
                fleet.state,
            ));
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successfulFleetDeletionSet", &successful),
        ec2_list("unsuccessfulFleetDeletionSet", &unsuccessful)
    );
    Ok(Ec2Service::respond("DeleteFleets", &req.request_id, &body))
}

pub(crate) fn describe_fleets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "FleetId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .fleets
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| {
            format!(
                "{}{}{}<targetCapacitySpecification><totalTargetCapacity>{}</totalTargetCapacity></targetCapacitySpecification>",
                ec2_elem("fleetId", &f.id),
                ec2_elem("fleetState", &f.state),
                ec2_elem("type", &f.fleet_type),
                f.target_capacity,
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeFleets",
        &req.request_id,
        &ec2_list("fleetSet", &items),
    ))
}

pub(crate) fn modify_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FleetId")?;
    validate_enum(
        &req.query_params,
        "ExcessCapacityTerminationPolicy",
        &["no-termination", "termination"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let fleet = state.fleets.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidFleetId.NotFound",
                format!("The fleet ID '{id}' does not exist"),
            )
        })?;
        if let Some(tc) = req
            .query_params
            .get("TargetCapacitySpecification.TotalTargetCapacity")
            .and_then(|v| v.parse::<i64>().ok())
        {
            fleet.target_capacity = tc;
        }
    }
    Ok(Ec2Service::respond(
        "ModifyFleet",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_fleet_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FleetId")?;
    let start = require(&req.query_params, "StartTime")?;
    validate_enum(
        &req.query_params,
        "EventType",
        &["instance-change", "fleet-change", "service-error"],
    )?;
    let body = format!(
        "{}{}{}{}",
        ec2_elem("fleetId", &id),
        ec2_elem("startTime", &start),
        ec2_elem("lastEvaluatedTime", FIXED_TIME),
        ec2_list("historyRecordSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeFleetHistory",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_fleet_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FleetId")?;
    let body = format!(
        "{}{}",
        ec2_elem("fleetId", &id),
        ec2_list("activeInstanceSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeFleetInstances",
        &req.request_id,
        &body,
    ))
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn create_fleet_persists_target_capacity() {
        let svc = Ec2Service::new();
        let resp = create_fleet(
            &svc,
            &req(
                "CreateFleet",
                &[
                    ("TargetCapacitySpecification.TotalTargetCapacity", "10"),
                    ("Type", "maintain"),
                ],
            ),
        )
        .unwrap();
        let fleet_id = {
            let b = body(resp);
            b.split("<fleetId>")
                .nth(1)
                .unwrap()
                .split("</fleetId>")
                .next()
                .unwrap()
                .to_string()
        };
        let desc = body(describe_fleets(&svc, &req("DescribeFleets", &[])).unwrap());
        assert!(
            desc.contains("<totalTargetCapacity>10</totalTargetCapacity>"),
            "{desc}"
        );

        // Modify updates it.
        modify_fleet(
            &svc,
            &req(
                "ModifyFleet",
                &[
                    ("FleetId", &fleet_id),
                    ("TargetCapacitySpecification.TotalTargetCapacity", "25"),
                ],
            ),
        )
        .unwrap();
        let desc2 = body(describe_fleets(&svc, &req("DescribeFleets", &[])).unwrap());
        assert!(
            desc2.contains("<totalTargetCapacity>25</totalTargetCapacity>"),
            "{desc2}"
        );
    }

    #[test]
    fn modify_fleet_missing_errors() {
        let svc = Ec2Service::new();
        let err = err_of(modify_fleet(
            &svc,
            &req("ModifyFleet", &[("FleetId", "fleet-nope")]),
        ));
        assert_eq!(err.code(), "InvalidFleetId.NotFound");
    }

    #[test]
    fn launch_template_data_round_trips() {
        // bug-audit 2026-07-28 (cycle 7) E1: CreateLaunchTemplate accepted the
        // LaunchTemplateData blob, used it for the 200, then dropped it ->
        // DescribeLaunchTemplateVersions returned <launchTemplateData/> ->
        // aws_launch_template perpetual drift. Every written field must read back.
        let svc = Ec2Service::new();
        create_launch_template(
            &svc,
            &req(
                "CreateLaunchTemplate",
                &[
                    ("LaunchTemplateName", "web"),
                    ("LaunchTemplateData.ImageId", "ami-0abc"),
                    ("LaunchTemplateData.InstanceType", "t3.large"),
                    ("LaunchTemplateData.KeyName", "kp"),
                    ("LaunchTemplateData.EbsOptimized", "true"),
                    ("LaunchTemplateData.SecurityGroupId.1", "sg-1"),
                    ("LaunchTemplateData.SecurityGroupId.2", "sg-2"),
                    ("LaunchTemplateData.Monitoring.Enabled", "true"),
                    ("LaunchTemplateData.IamInstanceProfile.Name", "role-x"),
                    ("LaunchTemplateData.Placement.Tenancy", "dedicated"),
                    ("LaunchTemplateData.CpuOptions.CoreCount", "2"),
                    ("LaunchTemplateData.CpuOptions.ThreadsPerCore", "1"),
                    ("LaunchTemplateData.MetadataOptions.HttpTokens", "required"),
                    (
                        "LaunchTemplateData.BlockDeviceMapping.1.DeviceName",
                        "/dev/sda",
                    ),
                    (
                        "LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize",
                        "40",
                    ),
                    (
                        "LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType",
                        "gp3",
                    ),
                    (
                        "LaunchTemplateData.TagSpecification.1.ResourceType",
                        "instance",
                    ),
                    ("LaunchTemplateData.TagSpecification.1.Tag.1.Key", "Env"),
                    ("LaunchTemplateData.TagSpecification.1.Tag.1.Value", "prod"),
                ],
            ),
        )
        .unwrap();

        let desc = body(
            describe_launch_template_versions(
                &svc,
                &req(
                    "DescribeLaunchTemplateVersions",
                    &[("LaunchTemplateName", "web")],
                ),
            )
            .unwrap(),
        );
        for needle in [
            "<imageId>ami-0abc</imageId>",
            "<instanceType>t3.large</instanceType>",
            "<keyName>kp</keyName>",
            "<ebsOptimized>true</ebsOptimized>",
            "<securityGroupIdSet><item>sg-1</item><item>sg-2</item></securityGroupIdSet>",
            "<monitoring><enabled>true</enabled></monitoring>",
            "<iamInstanceProfile><name>role-x</name></iamInstanceProfile>",
            "<placement><tenancy>dedicated</tenancy></placement>",
            "<cpuOptions><coreCount>2</coreCount><threadsPerCore>1</threadsPerCore></cpuOptions>",
            "<metadataOptions><httpTokens>required</httpTokens></metadataOptions>",
            "<deviceName>/dev/sda</deviceName>",
            "<ebs><volumeSize>40</volumeSize><volumeType>gp3</volumeType></ebs>",
            "<resourceType>instance</resourceType>",
            "<key>Env</key><value>prod</value>",
        ] {
            assert!(desc.contains(needle), "missing {needle} in:\n{desc}");
        }
        assert!(
            !desc.contains("<launchTemplateData/>"),
            "data must not read back empty: {desc}"
        );
    }

    #[test]
    fn launch_template_version_data_round_trips() {
        // A second version carries its own data; describe returns both distinctly.
        let svc = Ec2Service::new();
        create_launch_template(
            &svc,
            &req(
                "CreateLaunchTemplate",
                &[
                    ("LaunchTemplateName", "app"),
                    ("LaunchTemplateData.InstanceType", "t3.micro"),
                ],
            ),
        )
        .unwrap();
        create_launch_template_version(
            &svc,
            &req(
                "CreateLaunchTemplateVersion",
                &[
                    ("LaunchTemplateName", "app"),
                    ("LaunchTemplateData.InstanceType", "m5.large"),
                ],
            ),
        )
        .unwrap();
        let desc = body(
            describe_launch_template_versions(
                &svc,
                &req(
                    "DescribeLaunchTemplateVersions",
                    &[("LaunchTemplateName", "app")],
                ),
            )
            .unwrap(),
        );
        assert!(
            desc.contains("<instanceType>t3.micro</instanceType>"),
            "{desc}"
        );
        assert!(
            desc.contains("<instanceType>m5.large</instanceType>"),
            "{desc}"
        );
    }

    #[test]
    fn spot_fleet_target_capacity_round_trips() {
        let svc = Ec2Service::new();
        let resp = request_spot_fleet(
            &svc,
            &req(
                "RequestSpotFleet",
                &[
                    ("SpotFleetRequestConfig.IamFleetRole", "arn:x"),
                    ("SpotFleetRequestConfig.TargetCapacity", "7"),
                ],
            ),
        )
        .unwrap();
        let _ = body(resp);
        let desc = body(
            describe_spot_fleet_requests(&svc, &req("DescribeSpotFleetRequests", &[])).unwrap(),
        );
        assert!(
            desc.contains("<targetCapacity>7</targetCapacity>"),
            "{desc}"
        );
    }
}

#[cfg(test)]
mod spot_placement_score_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn include_local_zones_is_boolean_and_does_not_change_the_scored_set() {
        let svc = Ec2Service::new();
        let base = body(
            get_spot_placement_scores(
                &svc,
                &req("GetSpotPlacementScores", &[("TargetCapacity", "5")]),
            )
            .unwrap(),
        );

        // fakecloud models no Local Zones, so the scored set is identical.
        for flag in ["true", "false"] {
            let scored = body(
                get_spot_placement_scores(
                    &svc,
                    &req(
                        "GetSpotPlacementScores",
                        &[("TargetCapacity", "5"), ("IncludeLocalZones", flag)],
                    ),
                )
                .unwrap(),
            );
            assert_eq!(scored, base, "IncludeLocalZones={flag}");
        }

        // A non-boolean value is still rejected rather than silently ignored.
        let err = err_of(get_spot_placement_scores(
            &svc,
            &req(
                "GetSpotPlacementScores",
                &[("TargetCapacity", "5"), ("IncludeLocalZones", "yes")],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }
}
