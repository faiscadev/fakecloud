//! EBS volumes, attachments, modifications, and account-level EBS encryption
//! defaults.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, not_found, paginate, parse_filters, require,
    validate_enum, validate_max_results, Filter,
};
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
        modification: None,
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
    let retention = state.recycle_bin_retention.volumes;
    let vol = state
        .volumes
        .get_mut(&id)
        .ok_or_else(|| not_found("InvalidVolume.NotFound", &id))?;
    // AWS rejects deleting an attached volume — it must be detached first.
    if vol.state == "in-use" || !vol.attachments.is_empty() {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "VolumeInUse",
            format!("Volume '{id}' is currently attached and cannot be deleted"),
        ));
    }
    if retention {
        // A recycle-bin retention rule covers volumes: soft-delete so it can be
        // listed and restored rather than destroying it.
        vol.state = "deleting".to_string();
        vol.in_recycle_bin = true;
    } else {
        state.volumes.remove(&id);
        state.tags.remove(&id);
    }
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
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "VolumeId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    // An explicitly-requested VolumeId that does not exist (or is in the
    // recycle bin) is a hard error on AWS, not a silently-empty result.
    for id in &wanted {
        if !state.volumes.get(id).is_some_and(|v| !v.in_recycle_bin) {
            return Err(not_found("InvalidVolume.NotFound", id));
        }
    }
    let mut items: Vec<String> = state
        .volumes
        .values()
        .filter(|v| !v.in_recycle_bin)
        .filter(|v| wanted.is_empty() || wanted.contains(&v.volume_id))
        .filter(|v| vol_match(v, state.tags_for(&v.volume_id), &filters))
        .map(|v| volume_xml(v, state.tags_for(&v.volume_id)))
        .collect();
    items.sort();
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = paginate(&items, next_token, max_results);
    let body = format!(
        "{}{}",
        ec2_list("volumeSet", &page),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond(
        "DescribeVolumes",
        &req.request_id,
        &body,
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
            "encrypted" => vec![v.encrypted.to_string()],
            "size" => vec![v.size.to_string()],
            "attachment.instance-id" => v
                .attachments
                .iter()
                .map(|a| a.instance_id.clone())
                .collect(),
            "attachment.status" => v.attachments.iter().map(|a| a.status.clone()).collect(),
            "attachment.device" => v.attachments.iter().map(|a| a.device.clone()).collect(),
            "attachment.delete-on-termination" => v
                .attachments
                .iter()
                .map(|a| a.delete_on_termination.to_string())
                .collect(),
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            "tag-value" => tags.iter().map(|t| t.value.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return false;
                }
            }
        };
        f.values
            .iter()
            .any(|val| candidates.iter().any(|c| filter_value_matches(val, c)))
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
        let v = state
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| not_found("InvalidVolume.NotFound", &volume_id))?;
        v.state = "in-use".to_string();
        v.attachments = vec![att.clone()];
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
        let v = state
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| not_found("InvalidVolume.NotFound", &volume_id))?;
        if let Some(a) = v.attachments.first() {
            att.instance_id = a.instance_id.clone();
            att.device = a.device.clone();
        }
        v.state = "available".to_string();
        v.attachments.clear();
    }
    Ok(Ec2Service::respond(
        "DetachVolume",
        &req.request_id,
        &attachment_xml(&att),
    ))
}

fn opt_elem(name: &str, v: Option<i64>) -> String {
    v.map(|n| format!("<{name}>{n}</{name}>"))
        .unwrap_or_default()
}

fn modification_xml(volume_id: &str, m: &crate::state::VolumeModification) -> String {
    let progress = if m.state == "completed" { 100 } else { 0 };
    format!(
        "{}<modificationState>{}</modificationState>\
         <originalSize>{}</originalSize>{}{}{}\
         <targetSize>{}</targetSize>{}{}{}\
         <progress>{}</progress><startTime>{}</startTime>",
        ec2_elem("volumeId", volume_id),
        m.state,
        m.original_size,
        opt_elem("originalIops", m.original_iops),
        opt_elem("originalThroughput", m.original_throughput),
        ec2_elem("originalVolumeType", &m.original_volume_type),
        m.target_size,
        opt_elem("targetIops", m.target_iops),
        opt_elem("targetThroughput", m.target_throughput),
        ec2_elem("targetVolumeType", &m.target_volume_type),
        progress,
        m.start_time,
    )
}

pub(crate) fn modify_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VolumeId")?;
    validate_enum(&req.query_params, "VolumeType", VOLUME_TYPES)?;
    let rendered = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .volumes
            .get_mut(&id)
            .ok_or_else(|| not_found("InvalidVolume.NotFound", &id))?;
        // Snapshot the pre-modification values, then apply and record the change
        // so DescribeVolumesModifications reflects real targets, not constants.
        let original_size = v.size;
        let original_iops = v.iops;
        let original_throughput = v.throughput;
        let original_volume_type = v.volume_type.clone();
        if let Some(sz) = req.query_params.get("Size").and_then(|s| s.parse().ok()) {
            v.size = sz;
        }
        if let Some(vt) = req.query_params.get("VolumeType") {
            v.volume_type = vt.clone();
        }
        if let Some(iops) = req.query_params.get("Iops").and_then(|s| s.parse().ok()) {
            v.iops = Some(iops);
        }
        if let Some(tp) = req
            .query_params
            .get("Throughput")
            .and_then(|s| s.parse().ok())
        {
            v.throughput = Some(tp);
        }
        let m = crate::state::VolumeModification {
            original_size,
            original_iops,
            original_throughput,
            original_volume_type,
            target_size: v.size,
            target_iops: v.iops,
            target_throughput: v.throughput,
            target_volume_type: v.volume_type.clone(),
            state: "completed".to_string(),
            start_time: FIXED_TIME.to_string(),
        };
        let xml = modification_xml(&id, &m);
        v.modification = Some(m);
        xml
    };
    Ok(Ec2Service::respond(
        "ModifyVolume",
        &req.request_id,
        &format!("<volumeModification>{rendered}</volumeModification>"),
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
        .filter_map(|v| {
            v.modification
                .as_ref()
                .map(|m| modification_xml(&v.volume_id, m))
        })
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
            v.state = "available".to_string();
        }
    }
    Ok(Ec2Service::respond(
        "RestoreVolumeFromRecycleBin",
        &req.request_id,
        &ec2_return(true),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn seed_volume(svc: &Ec2Service, id: &str, state_name: &str, attached: bool) {
        let mut accounts = svc.state.write();
        let st = accounts.get_or_create("000000000000");
        st.volumes.insert(
            id.to_string(),
            Volume {
                volume_id: id.into(),
                size: 8,
                snapshot_id: None,
                availability_zone: "us-east-1a".into(),
                state: state_name.into(),
                volume_type: "gp3".into(),
                iops: Some(3000),
                throughput: Some(125),
                encrypted: false,
                kms_key_id: None,
                multi_attach_enabled: false,
                auto_enable_io: false,
                attachments: if attached {
                    vec![VolumeAttachment {
                        volume_id: id.into(),
                        instance_id: "i-1".into(),
                        device: "/dev/sdf".into(),
                        status: "attached".into(),
                        delete_on_termination: false,
                    }]
                } else {
                    vec![]
                },
                in_recycle_bin: false,
                modification: None,
            },
        );
    }

    fn body_of(resp: AwsResponse) -> String {
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    #[test]
    fn delete_volume_rejects_nonexistent() {
        let svc = Ec2Service::new();
        let err = err_of(delete_volume(
            &svc,
            &req("DeleteVolume", &[("VolumeId", "vol-nope")]),
        ));
        assert_eq!(err.code(), "InvalidVolume.NotFound");
    }

    #[test]
    fn delete_volume_rejects_in_use() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "in-use", true);
        let err = err_of(delete_volume(
            &svc,
            &req("DeleteVolume", &[("VolumeId", "vol-1")]),
        ));
        assert_eq!(err.code(), "VolumeInUse");
        // Still present after the rejected delete.
        assert!(svc
            .state
            .read()
            .get("000000000000")
            .unwrap()
            .volumes
            .contains_key("vol-1"));
    }

    #[test]
    fn delete_volume_hard_deletes_without_retention_rule() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        delete_volume(&svc, &req("DeleteVolume", &[("VolumeId", "vol-1")])).unwrap();
        assert!(!svc
            .state
            .read()
            .get("000000000000")
            .unwrap()
            .volumes
            .contains_key("vol-1"));
    }

    #[test]
    fn delete_volume_routes_to_recycle_bin_with_retention_rule() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        svc.state
            .write()
            .get_or_create("000000000000")
            .recycle_bin_retention
            .volumes = true;
        delete_volume(&svc, &req("DeleteVolume", &[("VolumeId", "vol-1")])).unwrap();
        // Present, flagged, and listed in the recycle bin.
        let list = body_of(
            list_volumes_in_recycle_bin(&svc, &req("ListVolumesInRecycleBin", &[])).unwrap(),
        );
        assert!(list.contains("<volumeId>vol-1</volumeId>"), "{list}");
        // Hidden from normal DescribeVolumes.
        let desc = body_of(describe_volumes(&svc, &req("DescribeVolumes", &[])).unwrap());
        assert!(!desc.contains("<volumeId>vol-1</volumeId>"), "{desc}");
        // Restore brings it back.
        restore_volume_from_recycle_bin(
            &svc,
            &req("RestoreVolumeFromRecycleBin", &[("VolumeId", "vol-1")]),
        )
        .unwrap();
        let desc2 = body_of(describe_volumes(&svc, &req("DescribeVolumes", &[])).unwrap());
        assert!(desc2.contains("<volumeId>vol-1</volumeId>"), "{desc2}");
    }

    #[test]
    fn modify_volume_persists_iops_and_throughput() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        modify_volume(
            &svc,
            &req(
                "ModifyVolume",
                &[
                    ("VolumeId", "vol-1"),
                    ("Size", "100"),
                    ("Iops", "6000"),
                    ("Throughput", "250"),
                ],
            ),
        )
        .unwrap();
        {
            let st = svc.state.read();
            let v = &st.get("000000000000").unwrap().volumes["vol-1"];
            assert_eq!(v.size, 100);
            assert_eq!(v.iops, Some(6000));
            assert_eq!(v.throughput, Some(250));
        }
        let body = body_of(
            describe_volumes_modifications(&svc, &req("DescribeVolumesModifications", &[]))
                .unwrap(),
        );
        assert!(body.contains("<targetIops>6000</targetIops>"), "{body}");
        assert!(
            body.contains("<targetThroughput>250</targetThroughput>"),
            "{body}"
        );
        assert!(body.contains("<targetSize>100</targetSize>"), "{body}");
    }

    #[test]
    fn describe_volumes_paginates() {
        let svc = Ec2Service::new();
        // MaxResults must be within [5, 1000]; seed enough volumes to force a
        // second page at the minimum page size.
        for i in 0..6 {
            seed_volume(&svc, &format!("vol-{i}"), "available", false);
        }
        let body = body_of(
            describe_volumes(&svc, &req("DescribeVolumes", &[("MaxResults", "5")])).unwrap(),
        );
        assert!(body.contains("<nextToken>"), "expected a NextToken: {body}");
    }

    #[test]
    fn describe_volumes_rejects_max_results_zero() {
        // MaxResults=0 with >=1 volume previously produced a self-referential
        // NextToken=0 that looped forever (bug-hunt finding 1.1). Like its
        // sibling paginators, DescribeVolumes must reject out-of-range
        // MaxResults with InvalidParameterValue instead.
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        let err = err_of(describe_volumes(
            &svc,
            &req("DescribeVolumes", &[("MaxResults", "0")]),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn describe_volumes_rejects_max_results_above_max() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        let err = err_of(describe_volumes(
            &svc,
            &req("DescribeVolumes", &[("MaxResults", "1001")]),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn describe_volumes_explicit_missing_id_errors() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        let err = err_of(describe_volumes(
            &svc,
            &req("DescribeVolumes", &[("VolumeId.1", "vol-missing")]),
        ));
        assert_eq!(err.code(), "InvalidVolume.NotFound");
    }

    #[test]
    fn describe_volumes_no_id_empty_ok() {
        // No explicit id + no match => empty result, never an error.
        let svc = Ec2Service::new();
        let body = body_of(describe_volumes(&svc, &req("DescribeVolumes", &[])).unwrap());
        assert!(body.contains("<volumeSet"), "{body}");
    }

    #[test]
    fn describe_volumes_tag_value_filter() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "available", false);
        {
            let mut accounts = svc.state.write();
            let st = accounts.get_or_create("000000000000");
            st.tags.insert(
                "vol-1".to_string(),
                vec![Tag {
                    key: "env".into(),
                    value: "prod".into(),
                }],
            );
        }
        let body = body_of(
            describe_volumes(
                &svc,
                &req(
                    "DescribeVolumes",
                    &[("Filter.1.Name", "tag-value"), ("Filter.1.Value.1", "prod")],
                ),
            )
            .unwrap(),
        );
        assert!(body.contains("<volumeId>vol-1</volumeId>"), "{body}");
        // A non-matching tag-value excludes it.
        let empty = body_of(
            describe_volumes(
                &svc,
                &req(
                    "DescribeVolumes",
                    &[("Filter.1.Name", "tag-value"), ("Filter.1.Value.1", "dev")],
                ),
            )
            .unwrap(),
        );
        assert!(!empty.contains("<volumeId>vol-1</volumeId>"), "{empty}");
    }

    #[test]
    fn attach_volume_rejects_nonexistent() {
        let svc = Ec2Service::new();
        let err = err_of(attach_volume(
            &svc,
            &req(
                "AttachVolume",
                &[
                    ("VolumeId", "vol-nope"),
                    ("InstanceId", "i-1"),
                    ("Device", "/dev/sdf"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidVolume.NotFound");
    }

    #[test]
    fn describe_volumes_attachment_filters() {
        let svc = Ec2Service::new();
        seed_volume(&svc, "vol-1", "in-use", true);
        seed_volume(&svc, "vol-2", "available", false);

        // attachment.instance-id matches only the attached volume.
        let body = body_of(
            describe_volumes(
                &svc,
                &req(
                    "DescribeVolumes",
                    &[
                        ("Filter.1.Name", "attachment.instance-id"),
                        ("Filter.1.Value.1", "i-1"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(body.contains("<volumeId>vol-1</volumeId>"), "{body}");
        assert!(!body.contains("<volumeId>vol-2</volumeId>"), "{body}");

        // attachment.device and attachment.status also resolve.
        let body = body_of(
            describe_volumes(
                &svc,
                &req(
                    "DescribeVolumes",
                    &[
                        ("Filter.1.Name", "attachment.device"),
                        ("Filter.1.Value.1", "/dev/sdf"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(body.contains("<volumeId>vol-1</volumeId>"), "{body}");

        // A device that matches nothing filters out both volumes.
        let body = body_of(
            describe_volumes(
                &svc,
                &req(
                    "DescribeVolumes",
                    &[
                        ("Filter.1.Name", "attachment.status"),
                        ("Filter.1.Value.1", "detached"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(!body.contains("<volumeId>vol-1</volumeId>"), "{body}");
    }
}
