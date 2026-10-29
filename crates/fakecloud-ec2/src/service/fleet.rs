//! Launch templates (+ versions), Spot instance/fleet requests, EC2 fleets,
//! and the spot datafeed subscription.

use std::collections::BTreeMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::resource_quotas as rq;
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

fn lt_xml(t: &LaunchTemplate, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}<defaultVersionNumber>{}</defaultVersionNumber><latestVersionNumber>{}</latestVersionNumber>{}",
        ec2_elem("launchTemplateId", &t.id),
        ec2_elem("launchTemplateName", &t.name),
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem(
            "createdBy",
            &fakecloud_aws::arn::Arn::global_in(region, "iam", owner, "root").to_string(),
        ),
        t.default_version,
        super::launch_template::latest_existing_version(t),
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
        &lt_indexed(d, "SecurityGroup"),
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

    // Network interfaces (indexed list of structs).
    let mut nis: Vec<String> = Vec::new();
    let mut i = 1;
    while d
        .keys()
        .any(|k| k.starts_with(&format!("NetworkInterface.{i}.")))
    {
        let p = format!("NetworkInterface.{i}");
        let scalars = [
            ("AssociateCarrierIpAddress", "associateCarrierIpAddress"),
            ("AssociatePublicIpAddress", "associatePublicIpAddress"),
            ("DeleteOnTermination", "deleteOnTermination"),
            ("Description", "description"),
            ("DeviceIndex", "deviceIndex"),
            ("InterfaceType", "interfaceType"),
            ("Ipv6AddressCount", "ipv6AddressCount"),
            ("NetworkInterfaceId", "networkInterfaceId"),
            ("PrivateIpAddress", "privateIpAddress"),
            (
                "SecondaryPrivateIpAddressCount",
                "secondaryPrivateIpAddressCount",
            ),
            ("SubnetId", "subnetId"),
            ("NetworkCardIndex", "networkCardIndex"),
            ("Ipv4PrefixCount", "ipv4PrefixCount"),
            ("Ipv6PrefixCount", "ipv6PrefixCount"),
            ("PrimaryIpv6", "primaryIpv6"),
            ("EnaQueueCount", "enaQueueCount"),
        ]
        .iter()
        .filter_map(|(k, e)| d.get(&format!("{p}.{k}")).map(|v| ec2_elem(e, v)))
        .collect::<String>();
        let groups: Vec<String> = lt_indexed(d, &format!("{p}.SecurityGroupId"))
            .iter()
            .map(|g| g.to_string())
            .collect();
        let groups = if groups.is_empty() {
            String::new()
        } else {
            format!(
                "<groupSet>{}</groupSet>",
                groups
                    .iter()
                    .map(|g| ec2_elem("groupId", g))
                    .collect::<String>()
            )
        };
        nis.push(format!("{scalars}{groups}"));
        i += 1;
    }
    out.push_str(&ec2_list("networkInterfaceSet", &nis));
    // Private DNS name options.
    let dns = [
        ("PrivateDnsNameOptions.HostnameType", "hostnameType"),
        (
            "PrivateDnsNameOptions.EnableResourceNameDnsARecord",
            "enableResourceNameDnsARecord",
        ),
        (
            "PrivateDnsNameOptions.EnableResourceNameDnsAAAARecord",
            "enableResourceNameDnsAAAARecord",
        ),
    ]
    .iter()
    .filter_map(|(k, e)| d.get(*k).map(|v| ec2_elem(e, v)))
    .collect::<String>();
    out.push_str(&lt_struct("privateDnsNameOptions", dns));
    if let Some(v) = d.get("MaintenanceOptions.AutoRecovery") {
        out.push_str(&lt_struct(
            "maintenanceOptions",
            ec2_elem("autoRecovery", v),
        ));
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
    region: &str,
) -> String {
    format!(
        "{}{}<versionNumber>{}</versionNumber>{}{}<defaultVersion>{}</defaultVersion>{}",
        ec2_elem("launchTemplateId", &t.id),
        ec2_elem("launchTemplateName", &t.name),
        version,
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem(
            "createdBy",
            &fakecloud_aws::arn::Arn::global_in(region, "iam", owner, "root").to_string(),
        ),
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
        // Checked under the same write lock as the insert, so two concurrent
        // creates of one name cannot both succeed.
        if state
            .launch_templates
            .values()
            .any(|existing| existing.name == t.name)
        {
            return Err(AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidLaunchTemplateName.AlreadyExistsException",
                format!("Launch template name already in use: {}", t.name),
            ));
        }
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
            lt_xml(&t, &tags, &owner, &req.region)
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
    let (t, version, data) = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        // AWS answers an unknown template (by id or name) with its not-found
        // error rather than inventing a version.
        let key = super::launch_template::find_template(state, id.as_deref(), name.as_deref())?
            .id
            .clone();
        if let Some(t) = state.launch_templates.get_mut(&key) {
            super::launch_template::materialize_versions(t);
            // `SourceVersion`: the new version inherits the source version's
            // data, with the request's LaunchTemplateData winning (the same
            // precedence a launch applies to the request over the template).
            let data: BTreeMap<String, String> = match req.query_params.get("SourceVersion") {
                Some(src) => {
                    let n = super::launch_template::resolve_version(t, Some(src))?;
                    let source = t.versions.get(&n).cloned().unwrap_or_default();
                    let own: std::collections::HashMap<String, String> =
                        data.clone().into_iter().collect();
                    super::launch_template::merge_launch_template_data(&source, &own)
                        .into_iter()
                        .collect()
                }
                None => data.clone(),
            };
            t.latest_version += 1;
            let v = t.latest_version;
            t.versions.insert(v, data.clone());
            (t.clone(), v, data)
        } else {
            // find_template found it under this same lock.
            return Err(crate::service_helpers::not_found(
                "InvalidLaunchTemplateId.NotFound",
                &key,
            ));
        }
    };
    Ok(Ec2Service::respond(
        "CreateLaunchTemplateVersion",
        &req.request_id,
        &format!(
            "<launchTemplateVersion>{}</launchTemplateVersion>",
            lt_version_xml(&t, version, &owner, &data, &req.region)
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
    let state = accounts.regional_mut(&req.account_id, &req.region);
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
            lt_xml(&t, &tags, &owner, &req.region)
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let versions = indexed_list(&req.query_params, "LaunchTemplateVersion");
    let req_id = req.query_params.get("LaunchTemplateId").cloned();
    let req_name = req.query_params.get("LaunchTemplateName").cloned();
    let mut ok_items: Vec<String> = Vec::new();
    let mut err_items: Vec<String> = Vec::new();
    let err_item = |id: &str, name: &str, v: &str, code: &str, msg: String| {
        format!(
            "{}{}{}<responseError>{}{}</responseError>",
            ec2_elem("launchTemplateId", id),
            ec2_elem("launchTemplateName", name),
            ec2_elem("versionNumber", v),
            ec2_elem("code", code),
            ec2_elem("message", &msg),
        )
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let key = resolve_lt(state, req).map(|t| t.id);
        match key.and_then(|k| state.launch_templates.get_mut(&k)) {
            None => {
                let (code, what) = match (&req_id, &req_name) {
                    (Some(id), _) => ("launchTemplateIdDoesNotExist", format!("ID {id}")),
                    (None, Some(n)) => ("launchTemplateNameDoesNotExist", format!("name {n}")),
                    (None, None) => ("launchTemplateIdDoesNotExist", "ID".to_string()),
                };
                for v in &versions {
                    err_items.push(err_item(
                        req_id.as_deref().unwrap_or(""),
                        req_name.as_deref().unwrap_or(""),
                        v,
                        code,
                        format!(
                            "The specified launch template, with template {what}, does not exist"
                        ),
                    ));
                }
            }
            Some(t) => {
                for v in &versions {
                    let n = v
                        .parse::<i64>()
                        .ok()
                        .filter(|n| super::launch_template::version_exists(t, *n));
                    match n {
                        None => err_items.push(err_item(
                            &t.id,
                            &t.name,
                            v,
                            "launchTemplateVersionDoesNotExist",
                            format!("The specified launch template version, {v}, does not exist"),
                        )),
                        Some(n) if n == t.default_version => err_items.push(err_item(
                            &t.id,
                            &t.name,
                            v,
                            "unexpectedError",
                            "Cannot delete the default version of a launch template".to_string(),
                        )),
                        Some(n) => {
                            super::launch_template::materialize_versions(t);
                            t.versions.remove(&n);
                            ok_items.push(format!(
                                "{}{}<versionNumber>{n}</versionNumber>",
                                ec2_elem("launchTemplateId", &t.id),
                                ec2_elem("launchTemplateName", &t.name),
                            ));
                        }
                    }
                }
            }
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successfullyDeletedLaunchTemplateVersionSet", &ok_items),
        ec2_list("unsuccessfullyDeletedLaunchTemplateVersionSet", &err_items)
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .launch_templates
        .values()
        .filter(|t| wanted.is_empty() || wanted.contains(&t.id))
        .map(|t| lt_xml(t, state.tags_for(&t.id), &owner, &req.region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLaunchTemplates",
        &req.request_id,
        &ec2_list("launchTemplates", &items),
    ))
}

/// The existing version numbers of a template, ascending (templates persisted
/// before per-version data was recorded had every number up to the latest).
fn existing_versions(t: &LaunchTemplate) -> Vec<i64> {
    if t.versions.is_empty() {
        (1..=t.latest_version).collect()
    } else {
        t.versions.keys().copied().collect()
    }
}

pub(crate) fn describe_launch_template_versions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let empty_data = BTreeMap::new();
    let selectors = indexed_list(&req.query_params, "LaunchTemplateVersion");
    let bound = |key: &str| {
        req.query_params
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
    };
    let (min, max) = (bound("MinVersion"), bound("MaxVersion"));
    let render = |t: &LaunchTemplate, v: i64| {
        let data = t.versions.get(&v).unwrap_or(&empty_data);
        lt_version_xml(t, v, &owner, data, &req.region)
    };
    let named = req.query_params.contains_key("LaunchTemplateId")
        || req.query_params.contains_key("LaunchTemplateName");
    let items: Vec<String> = if named {
        match resolve_lt(state, req) {
            None => Vec::new(),
            Some(t) => {
                let mut wanted: Vec<i64> = if selectors.is_empty() {
                    existing_versions(&t)
                } else {
                    let mut out = Vec::new();
                    for sel in &selectors {
                        out.push(super::launch_template::resolve_version(&t, Some(sel))?);
                    }
                    out.sort_unstable();
                    out.dedup();
                    out
                };
                wanted.retain(|v| min.is_none_or(|m| *v > m) && max.is_none_or(|m| *v <= m));
                wanted.into_iter().map(|v| render(&t, v)).collect()
            }
        }
    } else {
        // Account-wide: the latest and/or default version of every template.
        let latest = selectors.iter().any(|s| s == "$Latest");
        let default = selectors.iter().any(|s| s == "$Default");
        state
            .launch_templates
            .values()
            .flat_map(|t| {
                let mut vs = Vec::new();
                if latest {
                    vs.push(super::launch_template::latest_existing_version(t));
                }
                if default && !vs.contains(&t.default_version) {
                    vs.push(t.default_version);
                }
                vs.into_iter().map(move |v| (t, v))
            })
            .map(|(t, v)| render(t, v))
            .collect()
    };
    Ok(Ec2Service::respond(
        "DescribeLaunchTemplateVersions",
        &req.request_id,
        &ec2_list("launchTemplateVersionSet", &items),
    ))
}

/// The `LaunchTemplateData` (flattened, as stored for a template version) that
/// relaunches an existing instance the way it was launched: AMI, type, key,
/// placement, monitoring, the primary network interface with its security
/// groups, IAM instance profile, metadata / CPU / credit / DNS options, its
/// EBS volumes and tags.
fn instance_launch_template_data(state: &Ec2State, id: &str) -> Option<BTreeMap<String, String>> {
    let inst = state.instances.get(id)?;
    let mut d = BTreeMap::new();
    let mut put = |k: &str, v: String| {
        d.insert(k.to_string(), v);
    };
    put("ImageId", inst.image_id.clone());
    put("InstanceType", inst.instance_type.clone());
    if let Some(k) = &inst.key_name {
        put("KeyName", k.clone());
    }
    put("Monitoring.Enabled", inst.monitoring.to_string());
    put("EbsOptimized", inst.ebs_optimized.to_string());
    put(
        "DisableApiTermination",
        inst.disable_api_termination.to_string(),
    );
    put("DisableApiStop", inst.disable_api_stop.to_string());
    put(
        "InstanceInitiatedShutdownBehavior",
        inst.instance_initiated_shutdown_behavior.clone(),
    );
    if let Some(u) = &inst.user_data {
        put("UserData", u.clone());
    }
    put("Placement.AvailabilityZone", inst.az.clone());
    put(
        "Placement.Tenancy",
        inst.placement_tenancy
            .clone()
            .unwrap_or_else(|| "default".to_string()),
    );
    if let Some(g) = &inst.placement_group_name {
        put("Placement.GroupName", g.clone());
    }
    if let Some(a) = &inst.placement_affinity {
        put("Placement.Affinity", a.clone());
    }
    let m = &inst.metadata_options;
    put("MetadataOptions.HttpTokens", m.http_tokens.clone());
    put(
        "MetadataOptions.HttpPutResponseHopLimit",
        m.http_put_response_hop_limit.to_string(),
    );
    put("MetadataOptions.HttpEndpoint", m.http_endpoint.clone());
    put(
        "MetadataOptions.HttpProtocolIpv6",
        m.http_protocol_ipv6.clone(),
    );
    put(
        "MetadataOptions.InstanceMetadataTags",
        m.instance_metadata_tags.clone(),
    );
    if let Some(c) = &inst.cpu_options {
        put("CpuOptions.CoreCount", c.core_count.to_string());
        put("CpuOptions.ThreadsPerCore", c.threads_per_core.to_string());
    }
    if let Some(c) = state.instance_credit_specs.get(id) {
        put("CreditSpecification.CpuCredits", c.clone());
    }
    put(
        "PrivateDnsNameOptions.HostnameType",
        inst.private_dns_hostname_type
            .clone()
            .unwrap_or_else(|| "ip-name".to_string()),
    );
    put(
        "PrivateDnsNameOptions.EnableResourceNameDnsARecord",
        inst.enable_resource_name_dns_a_record.to_string(),
    );
    put(
        "PrivateDnsNameOptions.EnableResourceNameDnsAAAARecord",
        inst.enable_resource_name_dns_aaaa_record.to_string(),
    );
    put(
        "MaintenanceOptions.AutoRecovery",
        inst.maintenance_options.auto_recovery.clone(),
    );
    if let Some(assoc) = state
        .iam_instance_profile_associations
        .values()
        .find(|a| a.instance_id == id && a.state != "disassociated")
    {
        put(
            "IamInstanceProfile.Arn",
            assoc.iam_instance_profile_arn.clone(),
        );
    }
    // The primary network interface carries the subnet, private IP and
    // security groups.
    put("NetworkInterface.1.DeviceIndex", "0".to_string());
    put("NetworkInterface.1.DeleteOnTermination", "true".to_string());
    put(
        "NetworkInterface.1.AssociatePublicIpAddress",
        inst.public_ip.is_some().to_string(),
    );
    if let Some(sn) = &inst.subnet_id {
        put("NetworkInterface.1.SubnetId", sn.clone());
    }
    // The private IP is the instance's own: a template that pinned it would
    // hand every launch from it the same address.
    for (i, g) in inst.security_group_ids.iter().enumerate() {
        put(
            &format!("NetworkInterface.1.SecurityGroupId.{}", i + 1),
            g.clone(),
        );
    }
    let mut volumes: Vec<(&crate::state::Volume, &crate::state::VolumeAttachment)> = state
        .volumes
        .values()
        .filter_map(|v| {
            v.attachments
                .iter()
                .find(|a| a.instance_id == id)
                .map(|a| (v, a))
        })
        .collect();
    volumes.sort_by(|a, b| a.1.device.cmp(&b.1.device));
    for (n, (v, a)) in volumes.iter().enumerate() {
        let p = format!("BlockDeviceMapping.{}", n + 1);
        put(&format!("{p}.DeviceName"), a.device.clone());
        put(&format!("{p}.Ebs.VolumeSize"), v.size.to_string());
        put(&format!("{p}.Ebs.VolumeType"), v.volume_type.clone());
        put(
            &format!("{p}.Ebs.DeleteOnTermination"),
            a.delete_on_termination.to_string(),
        );
        put(&format!("{p}.Ebs.Encrypted"), v.encrypted.to_string());
        if let Some(k) = &v.kms_key_id {
            put(&format!("{p}.Ebs.KmsKeyId"), k.clone());
        }
        if let Some(i) = v.iops {
            put(&format!("{p}.Ebs.Iops"), i.to_string());
        }
        if let Some(t) = v.throughput {
            put(&format!("{p}.Ebs.Throughput"), t.to_string());
        }
        if let Some(sn) = &v.snapshot_id {
            put(&format!("{p}.Ebs.SnapshotId"), sn.clone());
        }
    }
    // User tags (not `aws:` system tags, which a template cannot carry).
    let tags: Vec<&Tag> = state
        .tags_for(id)
        .iter()
        .filter(|t| !t.key.starts_with("aws:"))
        .collect();
    if !tags.is_empty() {
        put("TagSpecification.1.ResourceType", "instance".to_string());
        for (j, t) in tags.iter().enumerate() {
            put(
                &format!("TagSpecification.1.Tag.{}.Key", j + 1),
                t.key.clone(),
            );
            put(
                &format!("TagSpecification.1.Tag.{}.Value", j + 1),
                t.value.clone(),
            );
        }
    }
    Some(d)
}

pub(crate) fn get_launch_template_data(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let data = instance_launch_template_data(state, &id)
        .ok_or_else(|| crate::service_helpers::instance_not_found(&id))?;
    Ok(Ec2Service::respond(
        "GetLaunchTemplateData",
        &req.request_id,
        &render_lt_data(&data),
    ))
}

pub(crate) fn modify_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    // `DefaultVersion` is serialized as `SetDefaultVersion` on the wire.
    let new_default = req
        .query_params
        .get("SetDefaultVersion")
        .or_else(|| req.query_params.get("DefaultVersion"));
    if let (Some(t), Some(v)) = (resolve_lt(state, req), new_default) {
        // Only an existing version can become the default.
        let n = super::launch_template::resolve_version(&t, Some(v))?;
        if let Some(t) = state.launch_templates.get_mut(&t.id) {
            t.default_version = n;
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
                lt_xml(&t, &tags, &owner, &req.region)
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
    let instance_type = req
        .query_params
        .get("LaunchSpecification.InstanceType")
        .filter(|v| !v.is_empty())
        .cloned();
    // Each request's instance counts toward the Spot vCPU quota of its family.
    let spot_quota = instance_type
        .as_deref()
        .and_then(|t| rq::vcpu_quota(t, true));
    let spot_limit =
        spot_quota.and_then(|q| svc.enforced_count_quota(&req.account_id, &req.region, q));
    let mut rendered = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        if let (Some(quota), Some(t)) = (spot_quota, instance_type.as_deref()) {
            let adding = count
                .max(1)
                .saturating_mul(rq::launch_vcpus(t, spot_limit)?);
            rq::check_vcpus(quota, spot_limit, rq::instance_vcpus(state, quota), adding)?;
        }
        for _ in 0..count.max(1) {
            let id = gen_id("sir");
            let r = SpotRequest {
                id: id.clone(),
                state: "active".to_string(),
                request_type: rtype.clone(),
                spot_price: price.clone(),
                instance_type: instance_type.clone(),
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        accounts
            .regional_mut(&req.account_id, &req.region)
            .spot_fleets
            .insert(
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        accounts
            .regional_mut(&req.account_id, &req.region)
            .spot_datafeed = Some((bucket.clone(), prefix.clone()));
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
        accounts
            .regional_mut(&req.account_id, &req.region)
            .spot_datafeed = None;
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
        .regional(&req.account_id, &req.region)
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
        accounts
            .regional_mut(&req.account_id, &req.region)
            .fleets
            .insert(
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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

#[cfg(test)]
mod launch_template_version_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn template_with_versions(svc: &Ec2Service, versions: &[&str]) {
        create_launch_template(
            svc,
            &req(
                "CreateLaunchTemplate",
                &[
                    ("LaunchTemplateName", "web"),
                    ("LaunchTemplateData.InstanceType", versions[0]),
                    ("LaunchTemplateData.KeyName", "kp"),
                ],
            ),
        )
        .unwrap();
        for v in &versions[1..] {
            create_launch_template_version(
                svc,
                &req(
                    "CreateLaunchTemplateVersion",
                    &[
                        ("LaunchTemplateName", "web"),
                        ("LaunchTemplateData.InstanceType", v),
                    ],
                ),
            )
            .unwrap();
        }
    }

    fn version_numbers(xml: &str) -> Vec<String> {
        xml.split("<versionNumber>")
            .skip(1)
            .filter_map(|s| s.split("</versionNumber>").next())
            .map(String::from)
            .collect()
    }

    #[test]
    fn duplicate_template_name_is_rejected() {
        let svc = Ec2Service::new();
        template_with_versions(&svc, &["t3.micro"]);
        let err = err_of(create_launch_template(
            &svc,
            &req("CreateLaunchTemplate", &[("LaunchTemplateName", "web")]),
        ));
        assert_eq!(
            err.code(),
            "InvalidLaunchTemplateName.AlreadyExistsException"
        );
    }

    #[test]
    fn deleted_versions_are_gone_and_the_default_cannot_be_deleted() {
        let svc = Ec2Service::new();
        template_with_versions(&svc, &["t3.micro", "t3.small", "t3.large"]);
        let out = body(
            delete_launch_template_versions(
                &svc,
                &req(
                    "DeleteLaunchTemplateVersions",
                    &[
                        ("LaunchTemplateName", "web"),
                        ("LaunchTemplateVersion.1", "2"),
                        ("LaunchTemplateVersion.2", "1"),
                        ("LaunchTemplateVersion.3", "9"),
                    ],
                ),
            )
            .unwrap(),
        );
        let (ok, failed) = out
            .split_once("<unsuccessfullyDeletedLaunchTemplateVersionSet>")
            .unwrap();
        assert_eq!(version_numbers(ok), vec!["2"]);
        assert_eq!(version_numbers(failed), vec!["1", "9"]);
        assert!(failed.contains("<code>launchTemplateVersionDoesNotExist</code>"));

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
        assert_eq!(version_numbers(&desc), vec!["1", "3"]);
    }

    #[test]
    fn describe_versions_honors_selectors_and_bounds() {
        let svc = Ec2Service::new();
        template_with_versions(&svc, &["t3.micro", "t3.small", "t3.large", "m5.large"]);
        let describe = |q: &[(&str, &str)]| {
            let mut q = q.to_vec();
            q.push(("LaunchTemplateName", "web"));
            version_numbers(&body(
                describe_launch_template_versions(&svc, &req("DescribeLaunchTemplateVersions", &q))
                    .unwrap(),
            ))
        };
        assert_eq!(
            describe(&[("LaunchTemplateVersion.1", "$Latest")]),
            vec!["4"]
        );
        assert_eq!(
            describe(&[
                ("LaunchTemplateVersion.1", "$Default"),
                ("LaunchTemplateVersion.2", "3")
            ]),
            vec!["1", "3"]
        );
        assert_eq!(
            describe(&[("MinVersion", "1"), ("MaxVersion", "3")]),
            vec!["2", "3"]
        );
        let err = err_of(describe_launch_template_versions(
            &svc,
            &req(
                "DescribeLaunchTemplateVersions",
                &[
                    ("LaunchTemplateName", "web"),
                    ("LaunchTemplateVersion.1", "7"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidLaunchTemplateId.VersionNotFound");
    }

    #[test]
    fn set_default_version_and_source_version() {
        let svc = Ec2Service::new();
        template_with_versions(&svc, &["t3.micro", "t3.small"]);
        let out = body(
            modify_launch_template(
                &svc,
                &req(
                    "ModifyLaunchTemplate",
                    &[("LaunchTemplateName", "web"), ("SetDefaultVersion", "2")],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<defaultVersionNumber>2</defaultVersionNumber>"),
            "{out}"
        );
        let err = err_of(modify_launch_template(
            &svc,
            &req(
                "ModifyLaunchTemplate",
                &[("LaunchTemplateName", "web"), ("SetDefaultVersion", "8")],
            ),
        ));
        assert_eq!(err.code(), "InvalidLaunchTemplateId.VersionNotFound");

        // Version 3 from source version 1: inherits KeyName, overrides type.
        let out = body(
            create_launch_template_version(
                &svc,
                &req(
                    "CreateLaunchTemplateVersion",
                    &[
                        ("LaunchTemplateName", "web"),
                        ("SourceVersion", "1"),
                        ("LaunchTemplateData.InstanceType", "c5.large"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains("<versionNumber>3</versionNumber>"), "{out}");
        assert!(out.contains("<keyName>kp</keyName>"), "{out}");
        assert!(
            out.contains("<instanceType>c5.large</instanceType>"),
            "{out}"
        );
    }

    #[test]
    fn security_group_names_and_network_interfaces_round_trip() {
        let svc = Ec2Service::new();
        create_launch_template(
            &svc,
            &req(
                "CreateLaunchTemplate",
                &[
                    ("LaunchTemplateName", "net"),
                    ("LaunchTemplateData.SecurityGroup.1", "web"),
                    ("LaunchTemplateData.NetworkInterface.1.DeviceIndex", "0"),
                    ("LaunchTemplateData.NetworkInterface.1.SubnetId", "subnet-1"),
                    (
                        "LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1",
                        "sg-1",
                    ),
                ],
            ),
        )
        .unwrap();
        let desc = body(
            describe_launch_template_versions(
                &svc,
                &req(
                    "DescribeLaunchTemplateVersions",
                    &[("LaunchTemplateName", "net")],
                ),
            )
            .unwrap(),
        );
        assert!(
            desc.contains("<securityGroupSet><item>web</item></securityGroupSet>"),
            "{desc}"
        );
        assert!(
            desc.contains("<networkInterfaceSet><item><deviceIndex>0</deviceIndex><subnetId>subnet-1</subnetId><groupSet><groupId>sg-1</groupId></groupSet></item></networkInterfaceSet>"),
            "{desc}"
        );
    }

    #[tokio::test]
    async fn get_launch_template_data_reflects_the_instance() {
        let svc = Ec2Service::new();
        let out = body(
            super::super::instance::run_instances(
                &svc,
                &req(
                    "RunInstances",
                    &[
                        ("ImageId", "ami-9"),
                        ("InstanceType", "m5.large"),
                        ("MinCount", "1"),
                        ("MaxCount", "1"),
                        ("KeyName", "kp"),
                        ("BlockDeviceMapping.1.DeviceName", "/dev/xvda"),
                        ("BlockDeviceMapping.1.Ebs.VolumeSize", "11"),
                        ("TagSpecification.1.ResourceType", "instance"),
                        ("TagSpecification.1.Tag.1.Key", "Name"),
                        ("TagSpecification.1.Tag.1.Value", "box"),
                    ],
                ),
            )
            .await
            .unwrap(),
        );
        let id = out
            .split("<instanceId>")
            .nth(1)
            .and_then(|s| s.split("</instanceId>").next())
            .unwrap()
            .to_string();
        let data = body(
            get_launch_template_data(&svc, &req("GetLaunchTemplateData", &[("InstanceId", &id)]))
                .unwrap(),
        );
        for needle in [
            "<imageId>ami-9</imageId>",
            "<instanceType>m5.large</instanceType>",
            "<keyName>kp</keyName>",
            "<deviceName>/dev/xvda</deviceName>",
            "<volumeSize>11</volumeSize>",
            "<key>Name</key><value>box</value>",
            "<deviceIndex>0</deviceIndex>",
        ] {
            assert!(data.contains(needle), "missing {needle}: {data}");
        }
        let err = err_of(get_launch_template_data(
            &svc,
            &req(
                "GetLaunchTemplateData",
                &[("InstanceId", "i-0000000000000000f")],
            ),
        ));
        assert_eq!(err.code(), "InvalidInstanceID.NotFound");
    }

    #[test]
    fn version_of_unknown_template_is_not_found() {
        let svc = Ec2Service::new();
        let err = err_of(create_launch_template_version(
            &svc,
            &req(
                "CreateLaunchTemplateVersion",
                &[
                    ("LaunchTemplateName", "nope"),
                    ("LaunchTemplateData.ImageId", "ami-1"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidLaunchTemplateName.NotFoundException");
    }
}
