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
        ec2_elem("bootMode", "uefi"),
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

pub(crate) fn describe_image_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ImageId")?;
    let attr = require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", IMG_ATTRS)?;
    let attr_xml = match attr.as_str() {
        "description" => "<description><value>ami</value></description>".to_string(),
        "launchPermission" => ec2_list("launchPermission", &[]),
        "productCodes" => ec2_list("productCodes", &[]),
        "blockDeviceMapping" => ec2_list("blockDeviceMapping", &[]),
        "bootMode" => "<bootMode><value>uefi</value></bootMode>".to_string(),
        _ => String::new(),
    };
    Ok(Ec2Service::respond(
        "DescribeImageAttribute",
        &req.request_id,
        &format!("{}{}", ec2_elem("imageId", &id), attr_xml),
    ))
}

pub(crate) fn modify_image_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ImageId")?;
    validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
    Ok(Ec2Service::respond(
        "ModifyImageAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_image_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ImageId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", &["launchPermission"])?;
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
