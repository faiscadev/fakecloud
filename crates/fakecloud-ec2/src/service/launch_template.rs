//! Launch-template resolution for instance launches.
//!
//! `RunInstances`, Auto Scaling, and CloudFormation all launch an instance
//! "from a launch template" the same way EC2 does: resolve the referenced
//! template version (`$Default` when no version is given, `$Latest`, or an
//! explicit version number), then merge that version's `LaunchTemplateData`
//! underneath the launch request's own parameters, so anything the request
//! sets wins. Stored template data is the flattened `LaunchTemplateData.*`
//! query sub-map, whose member names line up with the `RunInstances` query
//! members (`BlockDeviceMapping.N.Ebs.*`, `TagSpecification.N.*`,
//! `NetworkInterface.N.*`, `MetadataOptions.*`, ...), so the merge works on
//! that flattened form and the rest of the launch path reads one parameter
//! map regardless of where each value came from.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;

use crate::state::{Ec2State, LaunchTemplate};

/// System tag AWS puts on every instance launched from a template: the id.
pub const LAUNCH_TEMPLATE_ID_TAG: &str = "aws:ec2launchtemplate:id";
/// System tag AWS puts on every instance launched from a template: the
/// resolved version number.
pub const LAUNCH_TEMPLATE_VERSION_TAG: &str = "aws:ec2launchtemplate:version";

/// A launch template version resolved for a launch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLaunchTemplate {
    pub id: String,
    pub name: String,
    /// The concrete version number `$Default` / `$Latest` resolved to.
    pub version: i64,
    /// That version's flattened `LaunchTemplateData` (prefix stripped).
    pub data: BTreeMap<String, String>,
}

fn ec2_error(code: &str, message: String) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, message)
}

/// Find the template a launch names, by id or by name (exactly one of them).
pub(crate) fn find_template<'a>(
    state: &'a Ec2State,
    id: Option<&str>,
    name: Option<&str>,
) -> Result<&'a LaunchTemplate, AwsServiceError> {
    let id = id.filter(|v| !v.is_empty());
    let name = name.filter(|v| !v.is_empty());
    match (id, name) {
        (Some(_), Some(_)) => Err(ec2_error(
            "InvalidParameterCombination",
            "Both a launch template ID and a launch template name were specified. \
             Specify either a launch template ID or a launch template name, but not both."
                .to_string(),
        )),
        (None, None) => Err(ec2_error(
            "MissingParameter",
            "The request must contain the parameter launchTemplateName or launchTemplateId"
                .to_string(),
        )),
        (Some(id), None) => {
            if !id.starts_with("lt-") {
                return Err(ec2_error(
                    "InvalidLaunchTemplateId.Malformed",
                    format!("The specified ID for the launch template, {id}, is malformed."),
                ));
            }
            state.launch_templates.get(id).ok_or_else(|| {
                ec2_error(
                    "InvalidLaunchTemplateId.NotFound",
                    format!(
                        "The specified launch template, with template ID {id}, does not exist."
                    ),
                )
            })
        }
        (None, Some(name)) => state
            .launch_templates
            .values()
            .find(|t| t.name == name)
            .ok_or_else(|| {
                ec2_error(
                    "InvalidLaunchTemplateName.NotFoundException",
                    format!(
                        "The specified launch template, with template name {name}, does not exist."
                    ),
                )
            }),
    }
}

/// Whether `version` exists on `t`. Templates persisted before per-version
/// data was recorded have an empty `versions` map; every number up to
/// `latest_version` existed on those.
pub(crate) fn version_exists(t: &LaunchTemplate, version: i64) -> bool {
    if t.versions.is_empty() {
        return (1..=t.latest_version).contains(&version);
    }
    t.versions.contains_key(&version)
}

/// The newest version that still exists: what `$Latest` resolves to and
/// `latestVersionNumber` reports. Version numbers keep counting up from
/// `latest_version` (a deleted newest version is not reused), but `$Latest`
/// names the highest one left.
pub(crate) fn latest_existing_version(t: &LaunchTemplate) -> i64 {
    t.versions
        .keys()
        .next_back()
        .copied()
        .unwrap_or(t.latest_version)
}

/// Give a template persisted before per-version data was recorded (empty
/// `versions`, every number up to `latest_version` implicitly existing) an
/// explicit entry per version, so adding or deleting one keeps the others.
pub(crate) fn materialize_versions(t: &mut LaunchTemplate) {
    if t.versions.is_empty() {
        t.versions = (1..=t.latest_version)
            .map(|v| (v, BTreeMap::new()))
            .collect();
    }
}

/// Resolve a version selector (`$Default` / absent, `$Latest`, or a number)
/// against a template, to a concrete existing version number.
pub(crate) fn resolve_version(
    t: &LaunchTemplate,
    version: Option<&str>,
) -> Result<i64, AwsServiceError> {
    let selector = version.map(str::trim).filter(|v| !v.is_empty());
    let n = match selector {
        None | Some("$Default") => t.default_version,
        Some("$Latest") => latest_existing_version(t),
        Some(v) => v.parse::<i64>().map_err(|_| {
            ec2_error(
                "InvalidLaunchTemplateId.VersionNotFound",
                format!(
                    "The specified launch template version, {v}, does not exist for launch template {}.",
                    t.id
                ),
            )
        })?,
    };
    if !version_exists(t, n) {
        return Err(ec2_error(
            "InvalidLaunchTemplateId.VersionNotFound",
            format!(
                "The specified launch template version, {n}, does not exist for launch template {}.",
                t.id
            ),
        ));
    }
    Ok(n)
}

/// Resolve the launch template version a launch references.
pub fn resolve_launch_template(
    state: &Ec2State,
    id: Option<&str>,
    name: Option<&str>,
    version: Option<&str>,
) -> Result<ResolvedLaunchTemplate, AwsServiceError> {
    let t = find_template(state, id, name)?;
    let n = resolve_version(t, version)?;
    Ok(ResolvedLaunchTemplate {
        id: t.id.clone(),
        name: t.name.clone(),
        version: n,
        data: t.versions.get(&n).cloned().unwrap_or_default(),
    })
}

/// Resolve against the shared multi-account EC2 state (the entry point Auto
/// Scaling and CloudFormation use).
pub fn resolve_launch_template_in(
    state: &crate::state::SharedEc2State,
    account_id: &str,
    region: &str,
    id: Option<&str>,
    name: Option<&str>,
    version: Option<&str>,
) -> Result<ResolvedLaunchTemplate, AwsServiceError> {
    let accounts = state.read();
    let empty = Ec2State::new(account_id, region);
    let s = accounts.regional(account_id, region).unwrap_or(&empty);
    resolve_launch_template(s, id, name, version)
}

/// Members that are lists (or structs whose fields only make sense together):
/// when the request sets any part of one, it replaces the template's whole
/// value instead of being merged key by key. Security group ids and names are
/// one parameter for this purpose.
fn override_group(key: &str) -> Option<&'static str> {
    let top = key.split('.').next().unwrap_or(key);
    Some(match top {
        "SecurityGroupId" | "SecurityGroup" => "SecurityGroups",
        "BlockDeviceMapping" => "BlockDeviceMapping",
        "NetworkInterface" => "NetworkInterface",
        "IamInstanceProfile" => "IamInstanceProfile",
        "ElasticGpuSpecification" => "ElasticGpuSpecification",
        "ElasticInferenceAccelerator" => "ElasticInferenceAccelerator",
        "LicenseSpecification" => "LicenseSpecification",
        "SecondaryInterface" => "SecondaryInterface",
        "InstanceMarketOptions" => "InstanceMarketOptions",
        "CapacityReservationSpecification" => "CapacityReservationSpecification",
        "InstanceRequirements" => "InstanceRequirements",
        _ => return None,
    })
}

/// Tag specifications of a flattened parameter map, grouped by resource type
/// in first-seen order, each a list of `(key, value)`.
pub(crate) fn tag_specs<'a, I>(params: I) -> Vec<(String, Vec<(String, String)>)>
where
    I: Fn(&str) -> Option<&'a String>,
{
    let mut out: Vec<(String, Vec<(String, String)>)> = Vec::new();
    let mut i = 1usize;
    while let Some(rt) = params(&format!("TagSpecification.{i}.ResourceType")) {
        let mut j = 1usize;
        let mut tags = Vec::new();
        while let Some(k) = params(&format!("TagSpecification.{i}.Tag.{j}.Key")) {
            let v = params(&format!("TagSpecification.{i}.Tag.{j}.Value"))
                .cloned()
                .unwrap_or_default();
            if !k.is_empty() {
                tags.push((k.clone(), v));
            }
            j += 1;
        }
        match out.iter_mut().find(|(r, _)| r == rt) {
            Some((_, existing)) => merge_tags(existing, tags),
            None => out.push((rt.clone(), tags)),
        }
        i += 1;
    }
    out
}

/// Merge `overrides` into `base` by key: an existing key takes the override's
/// value in place, a new key is appended.
fn merge_tags(base: &mut Vec<(String, String)>, overrides: Vec<(String, String)>) {
    for (k, v) in overrides {
        match base.iter_mut().find(|(bk, _)| *bk == k) {
            Some(slot) => slot.1 = v,
            None => base.push((k, v)),
        }
    }
}

/// Replace every `TagSpecification.*` key in `params` with `specs`, written
/// back as contiguous `TagSpecification.N` blocks.
pub(crate) fn write_tag_specs(
    params: &mut HashMap<String, String>,
    specs: &[(String, Vec<(String, String)>)],
) {
    params.retain(|k, _| !k.starts_with("TagSpecification."));
    let mut n = 0usize;
    for (rt, tags) in specs {
        if tags.is_empty() {
            continue;
        }
        n += 1;
        params.insert(format!("TagSpecification.{n}.ResourceType"), rt.clone());
        for (j, (k, v)) in tags.iter().enumerate() {
            params.insert(format!("TagSpecification.{n}.Tag.{}.Key", j + 1), k.clone());
            params.insert(
                format!("TagSpecification.{n}.Tag.{}.Value", j + 1),
                v.clone(),
            );
        }
    }
}

/// Add (or overwrite) one tag on `resource_type` in a parameter map's tag
/// specifications.
pub(crate) fn add_tag(
    params: &mut HashMap<String, String>,
    resource_type: &str,
    key: &str,
    value: &str,
) {
    let mut specs = tag_specs(|k| params.get(k));
    let entry = (key.to_string(), value.to_string());
    match specs.iter_mut().find(|(rt, _)| rt == resource_type) {
        Some((_, tags)) => merge_tags(tags, vec![entry]),
        None => specs.push((resource_type.to_string(), vec![entry])),
    }
    write_tag_specs(params, &specs);
}

/// Index (the `N` of `NetworkInterface.N`) of the primary network interface in
/// a parameter map: the one with `DeviceIndex` 0, else the first listed.
pub(crate) fn primary_network_interface(params: &HashMap<String, String>) -> Option<usize> {
    let indexes: BTreeSet<usize> = params
        .keys()
        .filter_map(|k| k.strip_prefix("NetworkInterface."))
        .filter_map(|rest| rest.split('.').next()?.parse().ok())
        .collect();
    indexes
        .iter()
        .copied()
        .find(|n| {
            params
                .get(&format!("NetworkInterface.{n}.DeviceIndex"))
                .is_some_and(|v| v == "0")
        })
        .or_else(|| indexes.iter().next().copied())
}

/// Merge a template version's data under a launch request's parameters,
/// returning the effective launch parameters. The request wins: a scalar or
/// struct member the request sets replaces the template's, a list member the
/// request sets (block-device mappings, network interfaces, security groups,
/// ...) replaces the template's whole list, and tags are merged per resource
/// type with the request's value winning on a shared key. When the template
/// supplies the network interfaces and the request names an instance-level
/// subnet or security group ids, those apply to the template's primary
/// interface (EC2 does not take both an interface list and instance-level
/// network settings).
pub fn merge_launch_template_data(
    template: &BTreeMap<String, String>,
    request: &HashMap<String, String>,
) -> HashMap<String, String> {
    let request_groups: BTreeSet<&'static str> =
        request.keys().filter_map(|k| override_group(k)).collect();
    let mut out = request.clone();
    for (k, v) in template {
        if k.starts_with("TagSpecification.") {
            continue;
        }
        if override_group(k).is_some_and(|g| request_groups.contains(g)) {
            continue;
        }
        out.entry(k.clone()).or_insert_with(|| v.clone());
    }

    // Tags: template tags per resource type, request tags merged over them.
    let mut specs = tag_specs(|k| template.get(k));
    for (rt, tags) in tag_specs(|k| request.get(k)) {
        match specs.iter_mut().find(|(r, _)| *r == rt) {
            Some((_, existing)) => merge_tags(existing, tags),
            None => specs.push((rt, tags)),
        }
    }
    write_tag_specs(&mut out, &specs);

    // Instance-level subnet / security groups onto the template's primary
    // network interface.
    let template_nis = !request_groups.contains("NetworkInterface")
        && template.keys().any(|k| k.starts_with("NetworkInterface."));
    if template_nis {
        if let Some(n) = primary_network_interface(&out) {
            if let Some(subnet) = out.remove("SubnetId") {
                out.insert(format!("NetworkInterface.{n}.SubnetId"), subnet);
            }
            let ids = crate::service_helpers::indexed_list(request, "SecurityGroupId");
            if !ids.is_empty() {
                let prefix = format!("NetworkInterface.{n}.SecurityGroupId.");
                out.retain(|k, _| !k.starts_with(&prefix) && !k.starts_with("SecurityGroupId."));
                for (i, id) in ids.iter().enumerate() {
                    out.insert(format!("{prefix}{}", i + 1), id.clone());
                }
            }
        }
    }
    out
}

/// The launch parameters a request resolves to: its own when it names no
/// launch template, else the referenced version's data merged underneath it
/// plus the `aws:ec2launchtemplate:*` system tags on the instance. Returns the
/// resolved template alongside.
pub(crate) fn effective_launch_params(
    state: &Ec2State,
    request: &HashMap<String, String>,
) -> Result<(HashMap<String, String>, Option<ResolvedLaunchTemplate>), AwsServiceError> {
    let id = request.get("LaunchTemplate.LaunchTemplateId");
    let name = request.get("LaunchTemplate.LaunchTemplateName");
    if id.is_none() && name.is_none() {
        if request.contains_key("LaunchTemplate.Version") {
            return Err(ec2_error(
                "MissingParameter",
                "The request must contain the parameter launchTemplateName or launchTemplateId"
                    .to_string(),
            ));
        }
        return Ok((request.clone(), None));
    }
    let resolved = resolve_launch_template(
        state,
        id.map(String::as_str),
        name.map(String::as_str),
        request.get("LaunchTemplate.Version").map(String::as_str),
    )?;
    let mut params = merge_launch_template_data(&resolved.data, request);
    add_tag(
        &mut params,
        "instance",
        LAUNCH_TEMPLATE_ID_TAG,
        &resolved.id,
    );
    add_tag(
        &mut params,
        "instance",
        LAUNCH_TEMPLATE_VERSION_TAG,
        &resolved.version.to_string(),
    );
    Ok((params, Some(resolved)))
}

/// The `(key, value)` tags a parameter map's tag specifications put on one
/// resource type.
#[cfg(test)]
pub(crate) fn tags_for(
    params: &HashMap<String, String>,
    resource_type: &str,
) -> Vec<(String, String)> {
    tag_specs(|k| params.get(k))
        .into_iter()
        .find(|(rt, _)| rt == resource_type)
        .map(|(_, t)| t)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(versions: &[(i64, &[(&str, &str)])], default: i64) -> LaunchTemplate {
        LaunchTemplate {
            id: "lt-0123456789abcdef0".into(),
            name: "web".into(),
            default_version: default,
            latest_version: versions.iter().map(|(v, _)| *v).max().unwrap_or(1),
            versions: versions
                .iter()
                .map(|(v, d)| {
                    (
                        *v,
                        d.iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect(),
                    )
                })
                .collect(),
        }
    }

    fn state_with(t: LaunchTemplate) -> Ec2State {
        let mut s = Ec2State::new("000000000000", "us-east-1");
        s.launch_templates.insert(t.id.clone(), t);
        s
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn code(e: AwsServiceError) -> String {
        e.code().to_string()
    }

    #[test]
    fn version_selectors_resolve_default_latest_and_explicit() {
        let s = state_with(template(
            &[
                (1, &[("InstanceType", "t3.micro")]),
                (2, &[("InstanceType", "t3.small")]),
                (3, &[("InstanceType", "t3.large")]),
            ],
            2,
        ));
        let id = Some("lt-0123456789abcdef0");
        let r = |v: Option<&str>| resolve_launch_template(&s, id, None, v).unwrap();
        assert_eq!(r(None).version, 2, "no version means $Default");
        assert_eq!(r(Some("$Default")).version, 2);
        assert_eq!(r(Some("$Latest")).version, 3);
        assert_eq!(r(Some("1")).version, 1);
        assert_eq!(r(Some("1")).data["InstanceType"], "t3.micro");
        // By name resolves the same template.
        let by_name = resolve_launch_template(&s, None, Some("web"), Some("$Latest")).unwrap();
        assert_eq!(by_name.id, "lt-0123456789abcdef0");
        assert_eq!(by_name.data["InstanceType"], "t3.large");
    }

    #[test]
    fn missing_template_and_version_errors() {
        let s = state_with(template(&[(1, &[])], 1));
        assert_eq!(
            code(
                resolve_launch_template(&s, Some("lt-0000000000000000f"), None, None).unwrap_err()
            ),
            "InvalidLaunchTemplateId.NotFound"
        );
        assert_eq!(
            code(resolve_launch_template(&s, Some("nope"), None, None).unwrap_err()),
            "InvalidLaunchTemplateId.Malformed"
        );
        assert_eq!(
            code(resolve_launch_template(&s, None, Some("other"), None).unwrap_err()),
            "InvalidLaunchTemplateName.NotFoundException"
        );
        assert_eq!(
            code(
                resolve_launch_template(&s, Some("lt-0123456789abcdef0"), None, Some("7"))
                    .unwrap_err()
            ),
            "InvalidLaunchTemplateId.VersionNotFound"
        );
        assert_eq!(
            code(
                resolve_launch_template(&s, Some("lt-0123456789abcdef0"), None, Some("abc"))
                    .unwrap_err()
            ),
            "InvalidLaunchTemplateId.VersionNotFound"
        );
        assert_eq!(
            code(
                resolve_launch_template(&s, Some("lt-0123456789abcdef0"), Some("web"), None)
                    .unwrap_err()
            ),
            "InvalidParameterCombination"
        );
    }

    #[test]
    fn materialized_legacy_versions_survive_a_new_version() {
        let mut t = template(&[], 1);
        t.latest_version = 3;
        t.versions.clear();
        materialize_versions(&mut t);
        t.versions.insert(4, BTreeMap::new());
        assert!((1..=4).all(|v| version_exists(&t, v)));
    }

    #[test]
    fn latest_skips_a_deleted_newest_version() {
        let mut t = template(&[(1, &[]), (2, &[]), (3, &[])], 1);
        t.versions.remove(&3);
        let s = state_with(t);
        let r = resolve_launch_template(&s, None, Some("web"), Some("$Latest")).unwrap();
        assert_eq!(r.version, 2);
    }

    #[test]
    fn deleted_version_is_not_resolvable() {
        let mut t = template(&[(1, &[]), (2, &[])], 1);
        t.versions.remove(&2);
        let s = state_with(t);
        assert_eq!(
            code(resolve_launch_template(&s, None, Some("web"), Some("2")).unwrap_err()),
            "InvalidLaunchTemplateId.VersionNotFound"
        );
    }

    #[test]
    fn request_wins_over_template_scalars_and_lists() {
        let data: BTreeMap<String, String> = [
            ("ImageId", "ami-tmpl"),
            ("InstanceType", "t3.micro"),
            ("KeyName", "tmpl-key"),
            ("SecurityGroupId.1", "sg-a"),
            ("SecurityGroupId.2", "sg-b"),
            ("BlockDeviceMapping.1.DeviceName", "/dev/xvda"),
            ("BlockDeviceMapping.1.Ebs.VolumeSize", "20"),
            ("BlockDeviceMapping.2.DeviceName", "/dev/xvdb"),
            ("BlockDeviceMapping.2.Ebs.VolumeSize", "50"),
            (
                "IamInstanceProfile.Arn",
                "arn:aws:iam::000000000000:instance-profile/a",
            ),
            ("MetadataOptions.HttpTokens", "required"),
            ("MetadataOptions.HttpPutResponseHopLimit", "2"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let req = params(&[
            ("InstanceType", "m5.large"),
            ("SecurityGroupId.1", "sg-req"),
            ("BlockDeviceMapping.1.DeviceName", "/dev/sdf"),
            ("BlockDeviceMapping.1.Ebs.VolumeSize", "100"),
            ("IamInstanceProfile.Name", "b"),
            ("MetadataOptions.HttpPutResponseHopLimit", "5"),
        ]);
        let out = merge_launch_template_data(&data, &req);
        assert_eq!(
            out["ImageId"], "ami-tmpl",
            "unset scalar comes from template"
        );
        assert_eq!(out["InstanceType"], "m5.large", "request scalar wins");
        assert_eq!(out["KeyName"], "tmpl-key");
        // Lists are replaced whole.
        assert_eq!(out["SecurityGroupId.1"], "sg-req");
        assert!(!out.contains_key("SecurityGroupId.2"));
        assert_eq!(out["BlockDeviceMapping.1.DeviceName"], "/dev/sdf");
        assert!(!out.contains_key("BlockDeviceMapping.2.DeviceName"));
        // The profile is one value: Name from the request, no stale Arn.
        assert_eq!(out["IamInstanceProfile.Name"], "b");
        assert!(!out.contains_key("IamInstanceProfile.Arn"));
        // Struct fields merge member by member.
        assert_eq!(out["MetadataOptions.HttpTokens"], "required");
        assert_eq!(out["MetadataOptions.HttpPutResponseHopLimit"], "5");
    }

    #[test]
    fn tags_merge_per_resource_type_with_request_winning() {
        let data: BTreeMap<String, String> = [
            ("TagSpecification.1.ResourceType", "instance"),
            ("TagSpecification.1.Tag.1.Key", "env"),
            ("TagSpecification.1.Tag.1.Value", "tmpl"),
            ("TagSpecification.1.Tag.2.Key", "team"),
            ("TagSpecification.1.Tag.2.Value", "core"),
            ("TagSpecification.2.ResourceType", "volume"),
            ("TagSpecification.2.Tag.1.Key", "backup"),
            ("TagSpecification.2.Tag.1.Value", "daily"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let req = params(&[
            ("TagSpecification.1.ResourceType", "instance"),
            ("TagSpecification.1.Tag.1.Key", "env"),
            ("TagSpecification.1.Tag.1.Value", "prod"),
            ("TagSpecification.1.Tag.2.Key", "Name"),
            ("TagSpecification.1.Tag.2.Value", "web-1"),
        ]);
        let out = merge_launch_template_data(&data, &req);
        let inst = tags_for(&out, "instance");
        assert_eq!(
            inst,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "core".to_string()),
                ("Name".to_string(), "web-1".to_string()),
            ]
        );
        assert_eq!(
            tags_for(&out, "volume"),
            vec![("backup".to_string(), "daily".to_string())]
        );
    }

    #[test]
    fn instance_level_network_settings_apply_to_template_primary_interface() {
        let data: BTreeMap<String, String> = [
            ("NetworkInterface.1.DeviceIndex", "0"),
            ("NetworkInterface.1.SubnetId", "subnet-tmpl"),
            ("NetworkInterface.1.SecurityGroupId.1", "sg-tmpl"),
            ("NetworkInterface.1.AssociatePublicIpAddress", "true"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let req = params(&[("SubnetId", "subnet-req"), ("SecurityGroupId.1", "sg-req")]);
        let out = merge_launch_template_data(&data, &req);
        assert_eq!(out["NetworkInterface.1.SubnetId"], "subnet-req");
        assert_eq!(out["NetworkInterface.1.SecurityGroupId.1"], "sg-req");
        assert_eq!(out["NetworkInterface.1.AssociatePublicIpAddress"], "true");
        assert!(!out.contains_key("SubnetId"));
        assert!(!out.contains_key("SecurityGroupId.1"));
    }

    #[test]
    fn effective_params_add_system_tags_and_pass_through_plain_requests() {
        let s = state_with(template(
            &[(1, &[("ImageId", "ami-1")]), (2, &[("ImageId", "ami-2")])],
            1,
        ));
        let plain = params(&[("ImageId", "ami-x")]);
        let (out, lt) = effective_launch_params(&s, &plain).unwrap();
        assert!(lt.is_none());
        assert_eq!(out, plain);

        let req = params(&[
            ("LaunchTemplate.LaunchTemplateName", "web"),
            ("LaunchTemplate.Version", "$Latest"),
        ]);
        let (out, lt) = effective_launch_params(&s, &req).unwrap();
        assert_eq!(lt.unwrap().version, 2);
        assert_eq!(out["ImageId"], "ami-2");
        let tags = tags_for(&out, "instance");
        assert!(tags.contains(&(
            LAUNCH_TEMPLATE_ID_TAG.to_string(),
            "lt-0123456789abcdef0".to_string()
        )));
        assert!(tags.contains(&(LAUNCH_TEMPLATE_VERSION_TAG.to_string(), "2".to_string())));
    }
}
