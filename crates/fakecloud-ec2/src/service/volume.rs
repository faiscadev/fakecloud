//! EBS volumes, attachments, modifications, and account-level EBS encryption
//! defaults.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, indexed_list, parse_filters, require, validate_enum, Filter};
use crate::state::{Ec2State, Tag, Volume, VolumeAttachment};

const VOLUME_TYPES: &[&str] = &["standard", "io1", "io2", "gp2", "sc1", "st1", "gp3"];
const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn attachment_xml(a: &VolumeAttachment) -> String {
    format!(
        "{}{}{}{}{}<deleteOnTermination>{}</deleteOnTermination>",
        ec2_elem("volumeId", &a.volume_id),
        ec2_elem("instanceId", &a.instance_id),
        ec2_elem("device", &a.device),
        ec2_elem("status", &a.status),
        ec2_elem("attachTime", FIXED_TIME),
        a.delete_on_termination,
    )
}

fn volume_xml(v: &Volume, tags: &[Tag]) -> String {
    let atts: Vec<String> = v.attachments.iter().map(attachment_xml).collect();
    let mut out = format!(
        "{}<size>{}</size>{}{}{}{}<encrypted>{}</encrypted><multiAttachEnabled>{}</multiAttachEnabled>{}{}",
        ec2_elem("volumeId", &v.volume_id),
        v.size,
        ec2_elem("availabilityZone", &v.availability_zone),
        ec2_elem("status", &v.state),
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem("volumeType", &v.volume_type),
        v.encrypted,
        v.multi_attach_enabled,
        ec2_list("attachmentSet", &atts),
        super::tags::tag_set_xml(tags),
    );
    if let Some(s) = &v.snapshot_id {
        out.push_str(&ec2_elem("snapshotId", s));
    }
    if let Some(i) = v.iops {
        out.push_str(&format!("<iops>{i}</iops>"));
    }
    if let Some(t) = v.throughput {
        out.push_str(&format!("<throughput>{t}</throughput>"));
    }
    if let Some(k) = &v.kms_key_id {
        out.push_str(&ec2_elem("kmsKeyId", k));
    }
    out
}

pub(crate) fn create_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "VolumeType", VOLUME_TYPES)?;
    let id = gen_id("vol");
    let v = Volume {
        volume_id: id.clone(),
        size: req
            .query_params
            .get("Size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(8),
        snapshot_id: req.query_params.get("SnapshotId").cloned(),
        availability_zone: req
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
            }),
        state: "available".to_string(),
        volume_type: req
            .query_params
            .get("VolumeType")
            .cloned()
            .unwrap_or_else(|| "gp3".to_string()),
        iops: Some(3000),
        throughput: Some(125),
        encrypted: req
            .query_params
            .get("Encrypted")
            .map(|v| v == "true")
            .unwrap_or(false),
        kms_key_id: req.query_params.get("KmsKeyId").cloned(),
        multi_attach_enabled: false,
        auto_enable_io: false,
        attachments: Vec::new(),
        in_recycle_bin: false,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "volume");
        let t = state.tags_for(&id).to_vec();
        state.volumes.insert(id.clone(), v.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVolume",
        &req.request_id,
        &volume_xml(&v, &tags),
    ))
}

pub(crate) fn delete_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VolumeId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.volumes.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVolume",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_volumes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "VolumeId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .volumes
        .values()
        .filter(|v| !v.in_recycle_bin)
        .filter(|v| wanted.is_empty() || wanted.contains(&v.volume_id))
        .filter(|v| vol_match(v, state.tags_for(&v.volume_id), &filters))
        .map(|v| volume_xml(v, state.tags_for(&v.volume_id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVolumes",
        &req.request_id,
        &ec2_list("volumeSet", &items),
    ))
}

fn vol_match(v: &Volume, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "volume-id" => vec![v.volume_id.clone()],
            "volume-type" => vec![v.volume_type.clone()],
            "status" => vec![v.state.clone()],
            "availability-zone" => vec![v.availability_zone.clone()],
            "snapshot-id" => v.snapshot_id.clone().into_iter().collect(),
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

pub(crate) fn attach_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let device = require(&req.query_params, "Device")?;
    let instance_id = require(&req.query_params, "InstanceId")?;
    let volume_id = require(&req.query_params, "VolumeId")?;
    let att = VolumeAttachment {
        volume_id: volume_id.clone(),
        instance_id,
        device,
        status: "attached".to_string(),
        delete_on_termination: false,
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(v) = state.volumes.get_mut(&volume_id) {
            v.state = "in-use".to_string();
            v.attachments = vec![att.clone()];
        }
    }
    Ok(Ec2Service::respond(
        "AttachVolume",
        &req.request_id,
        &attachment_xml(&att),
    ))
}

pub(crate) fn detach_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let volume_id = require(&req.query_params, "VolumeId")?;
    let mut att = VolumeAttachment {
        volume_id: volume_id.clone(),
        instance_id: req
            .query_params
            .get("InstanceId")
            .cloned()
            .unwrap_or_default(),
        device: req.query_params.get("Device").cloned().unwrap_or_default(),
        status: "detaching".to_string(),
        delete_on_termination: false,
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(v) = state.volumes.get_mut(&volume_id) {
            if let Some(a) = v.attachments.first() {
                att.instance_id = a.instance_id.clone();
                att.device = a.device.clone();
            }
            v.state = "available".to_string();
            v.attachments.clear();
        }
    }
    Ok(Ec2Service::respond(
        "DetachVolume",
        &req.request_id,
        &attachment_xml(&att),
    ))
}

fn modification_xml(volume_id: &str) -> String {
    format!(
        "{}<modificationState>modifying</modificationState><targetSize>16</targetSize>\
         <originalSize>8</originalSize><progress>0</progress><startTime>{}</startTime>",
        ec2_elem("volumeId", volume_id),
        FIXED_TIME,
    )
}

pub(crate) fn modify_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VolumeId")?;
    validate_enum(&req.query_params, "VolumeType", VOLUME_TYPES)?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(v) = state.volumes.get_mut(&id) {
            if let Some(sz) = req.query_params.get("Size").and_then(|s| s.parse().ok()) {
                v.size = sz;
            }
            if let Some(vt) = req.query_params.get("VolumeType") {
                v.volume_type = vt.clone();
            }
        }
    }
    Ok(Ec2Service::respond(
        "ModifyVolume",
        &req.request_id,
        &format!(
            "<volumeModification>{}</volumeModification>",
            modification_xml(&id)
        ),
    ))
}

pub(crate) fn describe_volumes_modifications(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "VolumeId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .volumes
        .values()
        .filter(|v| wanted.is_empty() || wanted.contains(&v.volume_id))
        .map(|v| modification_xml(&v.volume_id))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeVolumesModifications",
        &req.request_id,
        &ec2_list("volumeModificationSet", &items),
    ))
}

pub(crate) fn describe_volume_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "VolumeId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .volumes
        .values()
        .filter(|v| wanted.is_empty() || wanted.contains(&v.volume_id))
        .map(|v| {
            format!(
                "{}{}<volumeStatus><status>ok</status></volumeStatus>{}{}",
                ec2_elem("volumeId", &v.volume_id),
                ec2_elem("availabilityZone", &v.availability_zone),
                ec2_list("actionsSet", &[]),
                ec2_list("eventsSet", &[]),
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeVolumeStatus",
        &req.request_id,
        &ec2_list("volumeStatusSet", &items),
    ))
}

pub(crate) fn describe_volume_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VolumeId")?;
    let attribute = require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["autoEnableIO", "productCodes"],
    )?;
    let auto = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.volumes.get(&id).map(|v| v.auto_enable_io))
            .unwrap_or(false)
    };
    let attr_xml = match attribute.as_str() {
        "productCodes" => ec2_list("productCodes", &[]),
        _ => format!("<autoEnableIO><value>{auto}</value></autoEnableIO>"),
    };
    Ok(Ec2Service::respond(
        "DescribeVolumeAttribute",
        &req.request_id,
        &format!("{}{}", ec2_elem("volumeId", &id), attr_xml),
    ))
}

pub(crate) fn modify_volume_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VolumeId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(v) = state.volumes.get_mut(&id) {
            if let Some(a) = req.query_params.get("AutoEnableIO.Value") {
                v.auto_enable_io = a == "true";
            }
        }
    }
    Ok(Ec2Service::respond(
        "ModifyVolumeAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn enable_volume_io(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VolumeId")?;
    Ok(Ec2Service::respond(
        "EnableVolumeIO",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- EBS encryption defaults (account-level) ----

pub(crate) fn get_ebs_encryption_by_default(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let enabled = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .map(|s| s.ebs_encryption_default)
            .unwrap_or(false)
    };
    Ok(Ec2Service::respond(
        "GetEbsEncryptionByDefault",
        &req.request_id,
        &format!(
            "<ebsEncryptionByDefault>{enabled}</ebsEncryptionByDefault><sseType>sse-ebs</sseType>"
        ),
    ))
}

fn set_ebs_default(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    val: bool,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ebs_encryption_default = val;
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &format!("<ebsEncryptionByDefault>{val}</ebsEncryptionByDefault>"),
    ))
}

pub(crate) fn enable_ebs_encryption_by_default(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    set_ebs_default(svc, req, "EnableEbsEncryptionByDefault", true)
}
pub(crate) fn disable_ebs_encryption_by_default(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    set_ebs_default(svc, req, "DisableEbsEncryptionByDefault", false)
}

pub(crate) fn get_ebs_default_kms_key_id(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let key = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.ebs_default_kms_key_id.clone())
            .unwrap_or_else(|| "alias/aws/ebs".to_string())
    };
    Ok(Ec2Service::respond(
        "GetEbsDefaultKmsKeyId",
        &req.request_id,
        &ec2_elem("kmsKeyId", &key),
    ))
}

pub(crate) fn modify_ebs_default_kms_key_id(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let key = require(&req.query_params, "KmsKeyId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ebs_default_kms_key_id = Some(key.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyEbsDefaultKmsKeyId",
        &req.request_id,
        &ec2_elem("kmsKeyId", &key),
    ))
}

pub(crate) fn reset_ebs_default_kms_key_id(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ebs_default_kms_key_id = None;
    }
    Ok(Ec2Service::respond(
        "ResetEbsDefaultKmsKeyId",
        &req.request_id,
        &ec2_elem("kmsKeyId", "alias/aws/ebs"),
    ))
}

// ---- recycle bin ----

pub(crate) fn list_volumes_in_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .volumes
        .values()
        .filter(|v| v.in_recycle_bin)
        .map(|v| {
            format!(
                "{}<recycleBinEnterTime>{}</recycleBinEnterTime>",
                ec2_elem("volumeId", &v.volume_id),
                FIXED_TIME
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "ListVolumesInRecycleBin",
        &req.request_id,
        &ec2_list("volumeSet", &items),
    ))
}

pub(crate) fn restore_volume_from_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VolumeId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(v) = state.volumes.get_mut(&id) {
            v.in_recycle_bin = false;
        }
    }
    Ok(Ec2Service::respond(
        "RestoreVolumeFromRecycleBin",
        &req.request_id,
        &ec2_return(true),
    ))
}
