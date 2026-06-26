//! AMIs (images): registration, lifecycle, attributes, deprecation,
//! deregistration protection, store/restore tasks, recycle bin, block-public
//! -access, and allowed-images settings.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_enum, validate_length,
    validate_max_results, Filter,
};
use crate::state::{Ec2State, Image, Tag};

/// Shared `Name` (3..128) + `Description` (0..255) length checks.
fn validate_image_strings(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_length(&req.query_params, "Name", 3, 128)?;
    validate_length(&req.query_params, "Description", 0, 255)?;
    Ok(())
}

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn image_xml(i: &Image, tags: &[Tag], owner: &str) -> String {
    // Effective owner: the AMI's own owner_id when set (seeded public AMIs),
    // else the requesting account (user-registered AMIs are owned by creator).
    let owner_id = i.owner_id.as_deref().unwrap_or(owner);
    let creation_date = i.creation_date.as_deref().unwrap_or(FIXED_TIME);
    let root_device_name = i.root_device_name.as_deref().unwrap_or("/dev/xvda");
    let platform_details = i.platform.as_deref().unwrap_or("Linux/UNIX");
    let owner_alias_xml = i
        .owner_alias
        .as_deref()
        .map(|a| ec2_elem("imageOwnerAlias", a))
        .unwrap_or_default();
    // Windows AMIs additionally report `<platform>windows</platform>`.
    let platform_xml = if platform_details.eq_ignore_ascii_case("windows") {
        ec2_elem("platform", "windows")
    } else {
        String::new()
    };
    format!(
        "{}{}<imageState>{}</imageState>{}{}{}{}<isPublic>{}</isPublic>{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("imageId", &i.image_id),
        ec2_elem("imageLocation", &format!("{owner_id}/{}", i.name)),
        i.state,
        ec2_elem("imageOwnerId", owner_id),
        owner_alias_xml,
        ec2_elem("creationDate", creation_date),
        platform_xml,
        i.public,
        ec2_elem("architecture", &i.architecture),
        ec2_elem("imageType", "machine"),
        ec2_elem("name", &i.name),
        ec2_elem("description", &i.description),
        ec2_elem("rootDeviceType", "ebs"),
        ec2_elem("rootDeviceName", root_device_name),
        ec2_elem("virtualizationType", "hvm"),
        ec2_elem("hypervisor", "xen"),
        ec2_elem("platformDetails", platform_details),
        ec2_elem("bootMode", i.boot_mode.as_deref().unwrap_or("uefi")),
        format_args!(
            "<deregistrationProtection>{}</deregistrationProtection>",
            if i.deregistration_protection {
                "enabled"
            } else {
                "disabled"
            }
        ),
        ec2_list("productCodes", &[]),
        ec2_list("blockDeviceMapping", &[]),
        super::tags::tag_set_xml(tags),
    )
}

fn build_image(name: String, description: String, source_instance_id: Option<String>) -> Image {
    Image {
        image_id: gen_id("ami"),
        name,
        description,
        state: "available".to_string(),
        architecture: "x86_64".to_string(),
        public: false,
        source_instance_id,
        in_recycle_bin: false,
        deprecation_time: None,
        deregistration_protection: false,
        launch_permission_users: Vec::new(),
        launch_permission_groups: Vec::new(),
        boot_mode: None,
        owner_id: None,
        owner_alias: None,
        creation_date: None,
        root_device_name: None,
        platform: None,
    }
}

pub(crate) fn create_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let instance_id = require(&req.query_params, "InstanceId")?;
    let name = require(&req.query_params, "Name")?;
    validate_image_strings(req)?;
    validate_enum(
        &req.query_params,
        "SnapshotLocation",
        &["regional", "local"],
    )?;
    let img = build_image(
        name,
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        Some(instance_id),
    );
    let id = img.image_id.clone();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "image");
        state.images.insert(id.clone(), img);
    }
    Ok(Ec2Service::respond(
        "CreateImage",
        &req.request_id,
        &ec2_elem("imageId", &id),
    ))
}

pub(crate) fn register_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = require(&req.query_params, "Name")?;
    validate_image_strings(req)?;
    validate_length(&req.query_params, "UefiData", 0, 64000)?;
    validate_enum(
        &req.query_params,
        "Architecture",
        &["i386", "x86_64", "arm64", "x86_64_mac", "arm64_mac"],
    )?;
    validate_enum(
        &req.query_params,
        "BootMode",
        &["legacy-bios", "uefi", "uefi-preferred"],
    )?;
    validate_enum(&req.query_params, "TpmSupport", &["v2.0"])?;
    validate_enum(&req.query_params, "ImdsSupport", &["v2.0"])?;
    let mut img = build_image(
        name,
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        None,
    );
    if let Some(a) = req.query_params.get("Architecture") {
        img.architecture = a.clone();
    }
    let id = img.image_id.clone();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .images
            .insert(id.clone(), img);
    }
    Ok(Ec2Service::respond(
        "RegisterImage",
        &req.request_id,
        &ec2_elem("imageId", &id),
    ))
}

/// An AMI the caller does not own. The seeded public catalogue is owned by
/// amazon/Canonical (`owner_id` set to a foreign account); a user-registered
/// AMI has `owner_id == None` (owned by the requesting account). AWS rejects
/// DeregisterImage / ModifyImageAttribute on an AMI you don't own.
fn owned_by_other(img: &Image, account: &str) -> bool {
    img.owner_id.as_deref().is_some_and(|o| o != account)
}

fn ami_auth_failure(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "AuthFailure",
        format!("Not authorized for image: {id}"),
    )
}

pub(crate) fn deregister_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Reject deregistering a public AMI owned by amazon/Canonical (the seeded
    // catalogue) — AWS returns AuthFailure, and removing a seed would silently
    // break `data.aws_ami { owners=["amazon"] }` for this account.
    if let Some(img) = state.images.get(&id) {
        if owned_by_other(img, &req.account_id) {
            return Err(ami_auth_failure(&id));
        }
    }
    state.images.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeregisterImage",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_images(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "ImageId");
    let owner = req.account_id.clone();
    // `Owner.N` request params (distinct from the `owner-*` filters): values are
    // an account id, `self`, or an owner alias (`amazon` / `aws-marketplace`).
    // When present, only images whose effective owner matches are returned —
    // this is what lets `aws_ami` data sources with `owners = ["amazon"]`
    // resolve the seeded public catalogue instead of every image in the account.
    let owners = indexed_list(&req.query_params, "Owner");
    let accounts = svc.state.read();
    // Lazily build the seeded fallback ONLY for a not-yet-created account, so
    // the common existing-account path doesn't construct-and-discard a fresh
    // (default-network + AMI-catalogue) state on every DescribeImages.
    let empty;
    let state = match accounts.get(&req.account_id) {
        Some(s) => s,
        None => {
            empty = Ec2State::new(&req.account_id, &req.region);
            &empty
        }
    };
    let mut items: Vec<String> = state
        .images
        .values()
        .filter(|i| !i.in_recycle_bin)
        .filter(|i| wanted.is_empty() || wanted.contains(&i.image_id))
        .filter(|i| image_owner_match(i, &owner, &owners))
        .filter(|i| img_match(i, state.tags_for(&i.image_id), &filters))
        .map(|i| image_xml(i, state.tags_for(&i.image_id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeImages",
        &req.request_id,
        &ec2_list("imagesSet", &items),
    ))
}

/// EC2 filter glob: values support `*` (any run) and `?` (one char). AMI `name`
/// filters in real Terraform configs use a mid-string `*`
/// (e.g. `amzn2-ami-hvm-*-x86_64-gp2`), so a trailing-only wildcard is not
/// enough here.
fn glob_match(pattern: &str, text: &str) -> bool {
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern == text;
    }
    // Classic two-pointer wildcard match with backtracking on `*`.
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Match an image against the `Owner.N` request params. Empty = no constraint.
/// Each owner is an account id, `self`, or an alias (`amazon`/`aws-marketplace`).
fn image_owner_match(i: &Image, account: &str, owners: &[String]) -> bool {
    if owners.is_empty() {
        return true;
    }
    let effective_owner = i.owner_id.as_deref().unwrap_or(account);
    owners.iter().any(|o| match o.as_str() {
        "self" => i.owner_id.is_none() || i.owner_id.as_deref() == Some(account),
        other => other == effective_owner || Some(other) == i.owner_alias.as_deref(),
    })
}

fn img_match(i: &Image, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "image-id" => vec![i.image_id.clone()],
            "name" => vec![i.name.clone()],
            "description" => vec![i.description.clone()],
            "state" => vec![i.state.clone()],
            "architecture" => vec![i.architecture.clone()],
            "is-public" => vec![i.public.to_string()],
            "root-device-type" => vec!["ebs".to_string()],
            "virtualization-type" => vec!["hvm".to_string()],
            "hypervisor" => vec!["xen".to_string()],
            "image-type" => vec!["machine".to_string()],
            "owner-id" => vec![i.owner_id.clone().unwrap_or_default()],
            "owner-alias" => vec![i.owner_alias.clone().unwrap_or_default()],
            "platform" => i
                .platform
                .as_deref()
                .filter(|p| p.eq_ignore_ascii_case("windows"))
                .map(|_| vec!["windows".to_string()])
                .unwrap_or_default(),
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return true;
                }
            }
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| glob_match(v, c)))
    })
}

pub(crate) fn copy_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = require(&req.query_params, "Name")?;
    validate_image_strings(req)?;
    validate_length(&req.query_params, "ClientToken", 0, 128)?;
    require(&req.query_params, "SourceImageId")?;
    require(&req.query_params, "SourceRegion")?;
    let img = build_image(
        name,
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        None,
    );
    let id = img.image_id.clone();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .images
            .insert(id.clone(), img);
    }
    Ok(Ec2Service::respond(
        "CopyImage",
        &req.request_id,
        &ec2_elem("imageId", &id),
    ))
}

const IMG_ATTRS: &[&str] = &[
    "description",
    "kernel",
    "ramdisk",
    "launchPermission",
    "productCodes",
    "blockDeviceMapping",
    "sriovNetSupport",
    "bootMode",
    "tpmSupport",
    "uefiData",
    "lastLaunchedTime",
    "imdsSupport",
    "deregistrationProtection",
];

/// XML for a single `launchPermission` item.
fn launch_permission_item(user_id: Option<&str>, group: Option<&str>) -> String {
    let mut inner = String::new();
    if let Some(u) = user_id {
        inner.push_str(&ec2_elem("userId", u));
    }
    if let Some(g) = group {
        inner.push_str(&ec2_elem("group", g));
    }
    format!("<item>{inner}</item>")
}

/// Collect `LaunchPermission.{Add,Remove}.N.{UserId,Group}` modifications.
/// Returns (added_users, added_groups, removed_users, removed_groups).
fn launch_permission_mods(
    params: &std::collections::HashMap<String, String>,
) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
    let collect = |op: &str| -> (Vec<String>, Vec<String>) {
        let mut users = Vec::new();
        let mut groups = Vec::new();
        let mut i = 1usize;
        loop {
            let base = format!("LaunchPermission.{op}.{i}");
            let user = params
                .get(&format!("{base}.UserId"))
                .filter(|v| !v.is_empty());
            let group = params
                .get(&format!("{base}.Group"))
                .filter(|v| !v.is_empty());
            if user.is_none() && group.is_none() {
                break;
            }
            if let Some(u) = user {
                users.push(u.clone());
            }
            if let Some(g) = group {
                groups.push(g.clone());
            }
            i += 1;
        }
        (users, groups)
    };
    let (add_u, add_g) = collect("Add");
    let (rem_u, rem_g) = collect("Remove");
    (add_u, add_g, rem_u, rem_g)
}

pub(crate) fn describe_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    let attr = require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", IMG_ATTRS)?;

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let img = state
        .images
        .get(&id)
        .ok_or_else(|| crate::service_helpers::not_found("InvalidAMIID.NotFound", &id))?;

    let attr_xml = match attr.as_str() {
        "description" => format!(
            "<description>{}</description>",
            ec2_elem("value", &img.description)
        ),
        "launchPermission" => {
            let mut items: Vec<String> = img
                .launch_permission_users
                .iter()
                .map(|u| launch_permission_item(Some(u), None))
                .collect();
            items.extend(
                img.launch_permission_groups
                    .iter()
                    .map(|g| launch_permission_item(None, Some(g))),
            );
            format!("<launchPermission>{}</launchPermission>", items.join(""))
        }
        "productCodes" => ec2_list("productCodes", &[]),
        "blockDeviceMapping" => ec2_list("blockDeviceMapping", &[]),
        "bootMode" => format!(
            "<bootMode><value>{}</value></bootMode>",
            img.boot_mode.as_deref().unwrap_or("uefi")
        ),
        // kernel/ramdisk/sriovNetSupport/tpmSupport/uefiData/lastLaunchedTime/
        // imdsSupport: AWS returns the element with no inner <value> when unset.
        "deregistrationProtection" => format!(
            "<deregistrationProtection><value>{}</value></deregistrationProtection>",
            if img.deregistration_protection {
                "enabled"
            } else {
                "disabled"
            }
        ),
        other => format!("<{other}/>"),
    };
    Ok(Ec2Service::respond(
        "DescribeImageAttribute",
        &req.request_id,
        &format!("{}{}", ec2_elem("imageId", &id), attr_xml),
    ))
}

pub(crate) fn modify_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;

    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let img = state
        .images
        .get_mut(&id)
        .ok_or_else(|| crate::service_helpers::not_found("InvalidAMIID.NotFound", &id))?;

    // AWS rejects modifying an AMI you don't own (the seeded amazon/Canonical
    // public catalogue) with AuthFailure.
    if owned_by_other(img, &req.account_id) {
        return Err(ami_auth_failure(&id));
    }

    // The `Attribute` form (Attribute + Value + OperationType) and the
    // structured `LaunchPermission`/`Description` forms are mutually
    // exclusive in a single call; support both.
    let attribute = req.query_params.get("Attribute").map(String::as_str);

    // ── launchPermission ──
    let (add_u, add_g, rem_u, rem_g) = launch_permission_mods(&req.query_params);
    let legacy_lp = attribute == Some("launchPermission");
    if !add_u.is_empty() || !add_g.is_empty() || !rem_u.is_empty() || !rem_g.is_empty() || legacy_lp
    {
        validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
        // Legacy UserId.N / UserGroup.N form keyed by OperationType.
        let (mut add_users, mut add_groups, mut rem_users, mut rem_groups) =
            (add_u, add_g, rem_u, rem_g);
        if legacy_lp {
            let users = indexed_list(&req.query_params, "UserId");
            let groups = indexed_list(&req.query_params, "UserGroup");
            match req.query_params.get("OperationType").map(String::as_str) {
                Some("remove") => {
                    rem_users.extend(users);
                    rem_groups.extend(groups);
                }
                _ => {
                    add_users.extend(users);
                    add_groups.extend(groups);
                }
            }
        }
        for u in add_users {
            if !img.launch_permission_users.contains(&u) {
                img.launch_permission_users.push(u);
            }
        }
        for g in add_groups {
            if !img.launch_permission_groups.contains(&g) {
                img.launch_permission_groups.push(g);
            }
        }
        img.launch_permission_users
            .retain(|u| !rem_users.contains(u));
        img.launch_permission_groups
            .retain(|g| !rem_groups.contains(g));
        // The `all` group makes the AMI public, matching AWS.
        img.public = img.launch_permission_groups.iter().any(|g| g == "all");
    }

    // ── description ──
    if let Some(d) = req
        .query_params
        .get("Description.Value")
        .or_else(|| req.query_params.get("Description"))
    {
        img.description = d.clone();
    } else if attribute == Some("description") {
        if let Some(v) = req.query_params.get("Value") {
            img.description = v.clone();
        }
    }

    // ── bootMode ──
    if let Some(b) = req.query_params.get("BootMode.Value") {
        img.boot_mode = Some(b.clone());
    } else if attribute == Some("bootMode") {
        if let Some(v) = req.query_params.get("Value") {
            img.boot_mode = Some(v.clone());
        }
    }

    Ok(Ec2Service::respond(
        "ModifyImageAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_image_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", &["launchPermission"])?;
    {
        let mut accounts = svc.state.write();
        if let Some(img) = accounts.get_or_create(&req.account_id).images.get_mut(&id) {
            img.launch_permission_users.clear();
            img.launch_permission_groups.clear();
            img.public = false;
        }
    }
    Ok(Ec2Service::respond(
        "ResetImageAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

/// Simple require-ImageId + `<return>true</return>` handler.
pub(crate) fn attach_image_watermark(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let image_id = require(&req.query_params, "ImageId")?;
    let watermark_name = require(&req.query_params, "WatermarkName")?;
    validate_length(&req.query_params, "WatermarkName", 1, 255)?;

    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if !state.images.contains_key(&image_id) {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidAMIID.NotFound",
            format!("The image id '[{image_id}]' does not exist"),
        ));
    }
    let watermark_key = gen_id("wmk");
    state
        .image_watermarks
        .entry(image_id)
        .or_default()
        .insert(watermark_key.clone(), watermark_name);
    Ok(Ec2Service::respond(
        "AttachImageWatermark",
        &req.request_id,
        &ec2_elem("watermarkKey", &watermark_key),
    ))
}

pub(crate) fn detach_image_watermark(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let image_id = require(&req.query_params, "ImageId")?;
    let watermark_key = require(&req.query_params, "WatermarkKey")?;

    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if !state.images.contains_key(&image_id) {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidAMIID.NotFound",
            format!("The image id '[{image_id}]' does not exist"),
        ));
    }
    if let Some(marks) = state.image_watermarks.get_mut(&image_id) {
        marks.remove(&watermark_key);
    }
    Ok(Ec2Service::respond(
        "DetachImageWatermark",
        &req.request_id,
        &ec2_return(true),
    ))
}

fn image_ack(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    extra_required: &[&str],
    protect: Option<bool>,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    for k in extra_required {
        require(&req.query_params, k)?;
    }
    if let Some(p) = protect {
        let mut accounts = svc.state.write();
        if let Some(img) = accounts.get_or_create(&req.account_id).images.get_mut(&id) {
            img.deregistration_protection = p;
        }
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn enable_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(svc, req, "EnableImage", &[], None)
}
pub(crate) fn disable_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(svc, req, "DisableImage", &[], None)
}
pub(crate) fn enable_image_deprecation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(svc, req, "EnableImageDeprecation", &["DeprecateAt"], None)
}
pub(crate) fn disable_image_deprecation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(svc, req, "DisableImageDeprecation", &[], None)
}
pub(crate) fn enable_image_deregistration_protection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(
        svc,
        req,
        "EnableImageDeregistrationProtection",
        &[],
        Some(true),
    )
}
pub(crate) fn disable_image_deregistration_protection(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(
        svc,
        req,
        "DisableImageDeregistrationProtection",
        &[],
        Some(false),
    )
}
pub(crate) fn cancel_image_launch_permission(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    image_ack(svc, req, "CancelImageLaunchPermission", &[], None)
}
pub(crate) fn restore_image_from_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(img) = accounts.get_or_create(&req.account_id).images.get_mut(&id) {
            img.in_recycle_bin = false;
        }
    }
    Ok(Ec2Service::respond(
        "RestoreImageFromRecycleBin",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- block public access (account-level) ----

pub(crate) fn enable_image_block_public_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = require(&req.query_params, "ImageBlockPublicAccessState")?;
    validate_enum(
        &req.query_params,
        "ImageBlockPublicAccessState",
        &["block-new-sharing"],
    )?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .image_block_public_access = st.clone();
    }
    Ok(Ec2Service::respond(
        "EnableImageBlockPublicAccess",
        &req.request_id,
        &ec2_elem("imageBlockPublicAccessState", &st),
    ))
}

pub(crate) fn disable_image_block_public_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .image_block_public_access = "unblocked".to_string();
    }
    Ok(Ec2Service::respond(
        "DisableImageBlockPublicAccess",
        &req.request_id,
        &ec2_elem("imageBlockPublicAccessState", "unblocked"),
    ))
}

pub(crate) fn get_image_block_public_access_state(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .map(|s| s.image_block_public_access.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unblocked".to_string())
    };
    Ok(Ec2Service::respond(
        "GetImageBlockPublicAccessState",
        &req.request_id,
        &ec2_elem("imageBlockPublicAccessState", &st),
    ))
}

// ---- allowed images settings (account-level) ----

pub(crate) fn enable_allowed_images_settings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = require(&req.query_params, "AllowedImagesSettingsState")?;
    validate_enum(
        &req.query_params,
        "AllowedImagesSettingsState",
        &["enabled", "audit-mode"],
    )?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .allowed_images_settings = st.clone();
    }
    Ok(Ec2Service::respond(
        "EnableAllowedImagesSettings",
        &req.request_id,
        &ec2_elem("allowedImagesSettingsState", &st),
    ))
}

pub(crate) fn disable_allowed_images_settings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .allowed_images_settings = "disabled".to_string();
    }
    Ok(Ec2Service::respond(
        "DisableAllowedImagesSettings",
        &req.request_id,
        &ec2_elem("allowedImagesSettingsState", "disabled"),
    ))
}

pub(crate) fn get_allowed_images_settings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .map(|s| s.allowed_images_settings.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "disabled".to_string())
    };
    let criteria = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .map(|s| s.allowed_image_criteria.clone())
            .unwrap_or_default()
    };
    let criterion_items: Vec<String> = criteria
        .iter()
        // `ec2_list` wraps each element in `<item>`, which is the
        // ImageProviderList member name the SDK reads — so pass the raw
        // provider strings, not pre-wrapped elements.
        .map(|providers| ec2_list("imageProviderSet", providers))
        .collect();
    let body = format!(
        "{}{}",
        ec2_elem("state", &st),
        ec2_list("imageCriterionSet", &criterion_items)
    );
    Ok(Ec2Service::respond(
        "GetAllowedImagesSettings",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn replace_image_criteria_in_allowed_images_settings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // Parse `ImageCriterion.N.ImageProvider.M` into a list-of-provider-lists and
    // persist it so GetAllowedImagesSettings round-trips (was a no-op stub).
    let mut criteria: Vec<Vec<String>> = Vec::new();
    let mut n = 1;
    loop {
        let providers = indexed_list(
            &req.query_params,
            &format!("ImageCriterion.{n}.ImageProvider"),
        );
        // A criterion with no ImageProvider.M still counts if its prefix exists.
        let has_criterion = providers.is_empty()
            && req
                .query_params
                .keys()
                .any(|k| k.starts_with(&format!("ImageCriterion.{n}.")));
        if providers.is_empty() && !has_criterion {
            break;
        }
        criteria.push(providers);
        n += 1;
    }
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .allowed_image_criteria = criteria;
    }
    Ok(Ec2Service::respond(
        "ReplaceImageCriteriaInAllowedImagesSettings",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- recycle bin / tasks ----

pub(crate) fn list_images_in_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .images
        .values()
        .filter(|i| i.in_recycle_bin)
        .map(|i| {
            format!(
                "{}{}",
                ec2_elem("imageId", &i.image_id),
                ec2_elem("name", &i.name)
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "ListImagesInRecycleBin",
        &req.request_id,
        &ec2_list("imageSet", &items),
    ))
}

pub(crate) fn create_store_image_task(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    require(&req.query_params, "Bucket")?;
    Ok(Ec2Service::respond(
        "CreateStoreImageTask",
        &req.request_id,
        &ec2_elem("objectKey", &format!("{id}.bin")),
    ))
}

pub(crate) fn describe_store_image_tasks(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 200)?;
    Ok(Ec2Service::respond(
        "DescribeStoreImageTasks",
        &req.request_id,
        &ec2_list("storeImageTaskResultSet", &[]),
    ))
}

pub(crate) fn create_restore_image_task(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Bucket")?;
    require(&req.query_params, "ObjectKey")?;
    validate_length(&req.query_params, "Name", 3, 128)?;
    let name = req
        .query_params
        .get("Name")
        .cloned()
        .unwrap_or_else(|| "restored".to_string());
    let img = build_image(name, String::new(), None);
    let id = img.image_id.clone();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .images
            .insert(id.clone(), img);
    }
    Ok(Ec2Service::respond(
        "CreateRestoreImageTask",
        &req.request_id,
        &ec2_elem("imageId", &id),
    ))
}

pub(crate) fn describe_fast_launch_images(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 0, 200)?;
    Ok(Ec2Service::respond(
        "DescribeFastLaunchImages",
        &req.request_id,
        &ec2_list("fastLaunchImageSet", &[]),
    ))
}

#[cfg(test)]
mod tests {
    use super::{glob_match, image_owner_match};
    use crate::state::{Ec2State, Image};

    fn img(owner_id: Option<&str>, alias: Option<&str>) -> Image {
        Image {
            image_id: "ami-test".into(),
            name: "n".into(),
            description: "d".into(),
            state: "available".into(),
            architecture: "x86_64".into(),
            public: true,
            source_instance_id: None,
            in_recycle_bin: false,
            deprecation_time: None,
            deregistration_protection: false,
            launch_permission_users: vec![],
            launch_permission_groups: vec![],
            boot_mode: None,
            owner_id: owner_id.map(str::to_string),
            owner_alias: alias.map(str::to_string),
            creation_date: None,
            root_device_name: None,
            platform: None,
        }
    }

    #[test]
    fn glob_matches_mid_string_wildcard() {
        // The shape real Terraform aws_ami filters use.
        assert!(glob_match(
            "amzn2-ami-hvm-*-x86_64-gp2",
            "amzn2-ami-hvm-2.0.20240306.2-x86_64-gp2"
        ));
        assert!(glob_match(
            "al2023-ami-*-kernel-6.1-arm64",
            "al2023-ami-2023.4.20240319.1-kernel-6.1-arm64"
        ));
        assert!(!glob_match(
            "amzn2-ami-hvm-*-x86_64-gp2",
            "ubuntu-noble-24.04"
        ));
        assert!(glob_match(
            "ubuntu-*-24.04-*-server-*",
            "ubuntu-noble-24.04-amd64-server-20240423"
        ));
        // exact + single-char
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "other"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        // trailing star
        assert!(glob_match("pre*", "prefix-anything"));
    }

    #[test]
    fn owner_filter_resolves_amazon_alias_and_self() {
        let amazon = img(Some("137112412989"), Some("amazon"));
        let user = img(None, None); // user-registered -> owned by caller
        let acct = "123456789012";

        // owners=["amazon"] matches the alias, not the user AMI.
        assert!(image_owner_match(&amazon, acct, &["amazon".into()]));
        assert!(!image_owner_match(&user, acct, &["amazon".into()]));
        // owners=[<amazon owner id>] matches by id.
        assert!(image_owner_match(&amazon, acct, &["137112412989".into()]));
        // owners=["self"] matches the user AMI (None owner = caller), not amazon.
        assert!(image_owner_match(&user, acct, &["self".into()]));
        assert!(!image_owner_match(&amazon, acct, &["self".into()]));
        // owners=[caller account] matches the user AMI.
        assert!(image_owner_match(&user, acct, &[acct.into()]));
        // empty owners = no constraint.
        assert!(image_owner_match(&amazon, acct, &[]));
    }

    #[test]
    fn seeded_catalogue_is_filterable_by_owner_and_name() {
        let state = Ec2State::new("123456789012", "us-east-1");
        // The seed catalogue is present and amazon-owned entries resolve.
        let amazon_amis: Vec<&Image> = state
            .images
            .values()
            .filter(|i| image_owner_match(i, "123456789012", &["amazon".into()]))
            .collect();
        assert!(
            amazon_amis.len() >= 4,
            "expected the amazon-owned seeds, got {}",
            amazon_amis.len()
        );
        // The canonical Ubuntu name-wildcard a real aws_ami data source sends.
        let ubuntu = state.images.values().find(|i| {
            glob_match(
                "ubuntu/images/hvm-ssd*/ubuntu-jammy-22.04-amd64-server-*",
                &i.name,
            )
        });
        assert!(
            ubuntu.is_some(),
            "ubuntu 22.04 seed should match the TF name filter"
        );
        // most_recent ordering is well-defined: seeds carry distinct dates.
        let mut dates: Vec<&str> = state
            .images
            .values()
            .filter_map(|i| i.creation_date.as_deref())
            .collect();
        let n = dates.len();
        dates.sort();
        dates.dedup();
        assert!(
            dates.len() >= 5 && n >= 6,
            "seeds need distinct creation dates for most_recent"
        );
    }
}
