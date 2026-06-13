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
    format!(
        "{}{}<imageState>{}</imageState>{}{}<isPublic>{}</isPublic>{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("imageId", &i.image_id),
        ec2_elem("imageLocation", &format!("{owner}/{}", i.name)),
        i.state,
        ec2_elem("imageOwnerId", owner),
        ec2_elem("creationDate", FIXED_TIME),
        i.public,
        ec2_elem("architecture", &i.architecture),
        ec2_elem("imageType", "machine"),
        ec2_elem("name", &i.name),
        ec2_elem("description", &i.description),
        ec2_elem("rootDeviceType", "ebs"),
        ec2_elem("rootDeviceName", "/dev/xvda"),
        ec2_elem("virtualizationType", "hvm"),
        ec2_elem("hypervisor", "xen"),
        ec2_elem("platformDetails", "Linux/UNIX"),
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

pub(crate) fn deregister_image(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
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
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .images
        .values()
        .filter(|i| !i.in_recycle_bin)
        .filter(|i| wanted.is_empty() || wanted.contains(&i.image_id))
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

fn img_match(i: &Image, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "image-id" => vec![i.image_id.clone()],
            "name" => vec![i.name.clone()],
            "state" => vec![i.state.clone()],
            "architecture" => vec![i.architecture.clone()],
            "is-public" => vec![i.public.to_string()],
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
        f.values.iter().any(|v| candidates.iter().any(|c| c == v))
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
            let user = params.get(&format!("{base}.UserId")).filter(|v| !v.is_empty());
            let group = params.get(&format!("{base}.Group")).filter(|v| !v.is_empty());
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

    // The `Attribute` form (Attribute + Value + OperationType) and the
    // structured `LaunchPermission`/`Description` forms are mutually
    // exclusive in a single call; support both.
    let attribute = req.query_params.get("Attribute").map(String::as_str);

    // ── launchPermission ──
    let (add_u, add_g, rem_u, rem_g) = launch_permission_mods(&req.query_params);
    let legacy_lp = attribute == Some("launchPermission");
    if !add_u.is_empty()
        || !add_g.is_empty()
        || !rem_u.is_empty()
        || !rem_g.is_empty()
        || legacy_lp
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
        img.launch_permission_users.retain(|u| !rem_users.contains(u));
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
    let body = format!(
        "{}{}",
        ec2_elem("state", &st),
        ec2_list("imageCriterionSet", &[])
    );
    Ok(Ec2Service::respond(
        "GetAllowedImagesSettings",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn replace_image_criteria_in_allowed_images_settings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
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
