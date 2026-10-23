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

// ---- EBS encryption ----

/// The account's EBS default KMS key in `region`: the key
/// `ModifyEbsDefaultKmsKeyId` set or, when not customized, the AWS-managed
/// `aws/ebs` key for the region (minted on first use). `None` only without a
/// KMS hook and no customized key.
pub(crate) fn ebs_default_key(svc: &Ec2Service, account_id: &str, region: &str) -> Option<String> {
    let custom = svc
        .state
        .read()
        .regional(account_id, region)
        .and_then(|s| s.ebs_default_kms_key(region));
    custom.or_else(|| {
        fakecloud_core::delivery::aws_managed_kms_key_arn(
            svc.kms_hook.as_deref(),
            account_id,
            region,
            "ebs",
        )
    })
}

/// A caller-named EBS key as the ARN of the key it names (a key KMS does not
/// know is reported as given).
pub(crate) fn named_ebs_key(svc: &Ec2Service, account_id: &str, region: &str, key: &str) -> String {
    fakecloud_core::delivery::resolve_named_kms_key_arn(
        svc.kms_hook.as_deref(),
        key,
        account_id,
        region,
        "ebs",
    )
}

/// How a new EBS volume is encrypted, as AWS decides it: encrypted when the
/// caller asks, when the account has encryption by default on, or when the
/// source snapshot is encrypted. An encrypted volume uses the named key (as
/// its ARN), else the encrypted source snapshot's key, else the account's EBS
/// default key. Also returns the source snapshot's size (the volume's size
/// when none is requested). Reads EC2 state and resolves KMS keys, so call it
/// with no EC2 lock held.
pub(crate) fn new_volume_encryption(
    svc: &Ec2Service,
    account_id: &str,
    region: &str,
    requested: bool,
    named: Option<&str>,
    snapshot_id: Option<&str>,
) -> (bool, Option<String>, Option<i64>) {
    let (by_default, source) = {
        let accounts = svc.state.read();
        let state = accounts.regional(account_id, region);
        (
            state.is_some_and(|s| s.ebs_encryption_by_default(region)),
            snapshot_id
                .and_then(|id| state.and_then(|s| s.snapshots.get(id)))
                .map(|snap| (snap.encrypted, snap.kms_key_id.clone(), snap.volume_size)),
        )
    };
    let snapshot_size = source.as_ref().map(|(_, _, size)| *size);
    let (snapshot_encrypted, snapshot_key) = source
        .map(|(encrypted, key, _)| (encrypted, key))
        .unwrap_or((false, None));
    let named = named.filter(|k| !k.is_empty());
    if !(requested || by_default || snapshot_encrypted) {
        return (false, named.map(str::to_string), snapshot_size);
    }
    let key = match named {
        Some(key) => Some(named_ebs_key(svc, account_id, region, key)),
        None if snapshot_encrypted => {
            snapshot_key.or_else(|| ebs_default_key(svc, account_id, region))
        }
        None => ebs_default_key(svc, account_id, region),
    };
    (true, key, snapshot_size)
}

/// Per-type IOPS/throughput semantics, matching AWS: gp3 defaults 3000/125
/// and both are settable; io1/io2 take the requested Iops; gp2 derives IOPS
/// from size (3 IOPS/GiB, clamped 100-16000); st1/sc1/standard have neither.
fn volume_performance(
    volume_type: &str,
    size: i64,
    req_iops: Option<i64>,
    req_throughput: Option<i64>,
) -> (Option<i64>, Option<i64>) {
    let iops = match volume_type {
        "gp3" => Some(req_iops.unwrap_or(3000)),
        "io1" | "io2" => req_iops.or(Some(100)),
        "gp2" => Some((size * 3).clamp(100, 16000)),
        _ => None,
    };
    let throughput = if volume_type == "gp3" {
        Some(req_throughput.unwrap_or(125))
    } else {
        None
    };
    (iops, throughput)
}

/// One EBS volume a launch creates from a block-device mapping, with its
/// size, performance and encryption already resolved.
#[derive(Clone, Debug)]
pub(crate) struct LaunchVolume {
    pub device: String,
    pub size: i64,
    pub volume_type: String,
    pub iops: Option<i64>,
    pub throughput: Option<i64>,
    pub snapshot_id: Option<String>,
    pub encrypted: bool,
    pub kms_key_id: Option<String>,
    pub delete_on_termination: bool,
}

/// The EBS volumes a launch's `<prefix>.N.*` block-device mappings create
/// (`BlockDeviceMapping` on RunInstances). Only a mapping with an `Ebs` block
/// makes a volume -- an instance-store `VirtualName` or a `NoDevice`
/// suppression makes none. Each volume's encryption follows
/// [`new_volume_encryption`] (the mapping's `Ebs.Encrypted` / `Ebs.KmsKeyId`,
/// encryption by default, an encrypted source snapshot). Resolved once per
/// launch with no EC2 lock held, then created per instance by
/// [`create_launch_volumes`].
pub(crate) fn launch_volumes(
    svc: &Ec2Service,
    account_id: &str,
    region: &str,
    params: &std::collections::HashMap<String, String>,
    prefix: &str,
) -> Vec<LaunchVolume> {
    let mut indexes: Vec<u32> = params
        .keys()
        .filter_map(|k| k.strip_prefix(prefix)?.strip_prefix('.'))
        .filter_map(|rest| rest.split('.').next()?.parse().ok())
        .collect();
    indexes.sort_unstable();
    indexes.dedup();
    indexes
        .into_iter()
        .filter_map(|n| {
            let get = |field: &str| params.get(&format!("{prefix}.{n}.{field}"));
            let has_ebs = params
                .keys()
                .any(|k| k.starts_with(&format!("{prefix}.{n}.Ebs.")));
            if !has_ebs || get("NoDevice").is_some() {
                return None;
            }
            let device = get("DeviceName").cloned().unwrap_or_default();
            let snapshot_id = get("Ebs.SnapshotId").filter(|s| !s.is_empty()).cloned();
            let (encrypted, kms_key_id, snapshot_size) = new_volume_encryption(
                svc,
                account_id,
                region,
                get("Ebs.Encrypted").is_some_and(|v| v == "true"),
                get("Ebs.KmsKeyId").map(String::as_str),
                snapshot_id.as_deref(),
            );
            let size = get("Ebs.VolumeSize")
                .and_then(|v| v.parse().ok())
                .or(snapshot_size)
                .unwrap_or(8);
            let volume_type = get("Ebs.VolumeType")
                .filter(|v| !v.is_empty())
                .cloned()
                .unwrap_or_else(|| "gp3".to_string());
            let (iops, throughput) = volume_performance(
                &volume_type,
                size,
                get("Ebs.Iops").and_then(|v| v.parse().ok()),
                get("Ebs.Throughput").and_then(|v| v.parse().ok()),
            );
            Some(LaunchVolume {
                device,
                size,
                volume_type,
                iops,
                throughput,
                snapshot_id,
                encrypted,
                kms_key_id,
                delete_on_termination: get("Ebs.DeleteOnTermination").is_none_or(|v| v != "false"),
            })
        })
        .collect()
}

/// Create `volumes` for a launched instance, attached (`in-use`) at their
/// device names in the instance's AZ, as RunInstances does. `tag_params` carries the launch's `TagSpecification.N.*`; its `volume`
/// specifications tag each created volume.
pub(crate) fn create_launch_volumes(
    state: &mut Ec2State,
    instance_id: &str,
    availability_zone: &str,
    volumes: &[LaunchVolume],
    tag_params: &std::collections::HashMap<String, String>,
) {
    for lv in volumes {
        let volume_id = gen_id("vol");
        crate::service::tags::apply_tag_specifications(state, tag_params, &volume_id, "volume");
        state.volumes.insert(
            volume_id.clone(),
            Volume {
                volume_id: volume_id.clone(),
                size: lv.size,
                snapshot_id: lv.snapshot_id.clone(),
                availability_zone: availability_zone.to_string(),
                state: "in-use".to_string(),
                volume_type: lv.volume_type.clone(),
                iops: lv.iops,
                throughput: lv.throughput,
                encrypted: lv.encrypted,
                kms_key_id: lv.kms_key_id.clone(),
                multi_attach_enabled: false,
                auto_enable_io: false,
                attachments: vec![VolumeAttachment {
                    volume_id,
                    instance_id: instance_id.to_string(),
                    device: lv.device.clone(),
                    status: "attached".to_string(),
                    delete_on_termination: lv.delete_on_termination,
                }],
                in_recycle_bin: false,
                modification: None,
            },
        );
    }
}

/// Release the volumes attached to terminated `instance_ids`: a volume whose
/// attachment has `DeleteOnTermination` is deleted (to the recycle bin when a
/// retention rule covers volumes, as DeleteVolume does); any other is
/// detached and becomes `available`.
pub(crate) fn release_terminated_volumes(state: &mut Ec2State, instance_ids: &[String]) {
    let retention = state.recycle_bin_retention.volumes;
    let mut deleted = Vec::new();
    for vol in state.volumes.values_mut() {
        let (gone, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut vol.attachments)
            .into_iter()
            .partition(|a| instance_ids.contains(&a.instance_id));
        vol.attachments = kept;
        if gone.is_empty() {
            continue;
        }
        if gone.iter().any(|a| a.delete_on_termination) && vol.attachments.is_empty() {
            if retention {
                vol.state = "deleting".to_string();
                vol.in_recycle_bin = true;
            } else {
                deleted.push(vol.volume_id.clone());
            }
        } else if vol.attachments.is_empty() {
            vol.state = "available".to_string();
        }
    }
    for id in deleted {
        state.volumes.remove(&id);
        state.tags.remove(&id);
    }
}

pub(crate) fn create_volume(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "VolumeType", VOLUME_TYPES)?;
    let id = gen_id("vol");
    let snapshot_id = req.query_params.get("SnapshotId").cloned();
    let (encrypted, kms_key_id, snapshot_size) = new_volume_encryption(
        svc,
        &req.account_id,
        &req.region,
        req.query_params
            .get("Encrypted")
            .is_some_and(|v| v == "true"),
        req.query_params.get("KmsKeyId").map(String::as_str),
        snapshot_id.as_deref(),
    );
    // A volume from a snapshot defaults to the snapshot's size.
    let size: i64 = req
        .query_params
        .get("Size")
        .and_then(|s| s.parse().ok())
        .or(snapshot_size)
        .unwrap_or(8);
    let volume_type = req
        .query_params
        .get("VolumeType")
        .cloned()
        .unwrap_or_else(|| "gp3".to_string());
    let req_iops = req
        .query_params
        .get("Iops")
        .and_then(|s| s.parse::<i64>().ok());
    let req_throughput = req
        .query_params
        .get("Throughput")
        .and_then(|s| s.parse::<i64>().ok());
    let (iops, throughput) = volume_performance(&volume_type, size, req_iops, req_throughput);
    let multi_attach_enabled = req
        .query_params
        .get("MultiAttachEnabled")
        .map(|v| v == "true")
        .unwrap_or(false);
    let v = Volume {
        volume_id: id.clone(),
        size,
        snapshot_id,
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
        volume_type,
        iops,
        throughput,
        encrypted,
        kms_key_id,
        multi_attach_enabled,
        auto_enable_io: false,
        attachments: Vec::new(),
        in_recycle_bin: false,
        modification: None,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    // An explicitly-requested VolumeId that does not exist (or is in the
    // recycle bin) is a hard error on AWS, not a silently-empty result.
    for id in &wanted {
        if state.volumes.get(id).is_none_or(|v| v.in_recycle_bin) {
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
    let (page, token) = paginate(&items, next_token, max_results)?;
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
            .regional(&req.account_id, &req.region)
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
            .regional(&req.account_id, &req.region)
            .map(|s| s.ebs_encryption_by_default(&req.region))
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
            .regional_mut(&req.account_id, &req.region)
            .set_ebs_encryption_by_default(&req.region, val);
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
    // The ARN of the key new encrypted volumes use: the customized key, or
    // the AWS-managed `aws/ebs` key for the region.
    let key = ebs_default_key(svc, &req.account_id, &req.region)
        .unwrap_or_else(|| "alias/aws/ebs".to_string());
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
    let named = require(&req.query_params, "KmsKeyId")?;
    // Stored and reported as the ARN of the key named (an alias or key id
    // resolves through KMS), which is what new encrypted volumes then report.
    let key = named_ebs_key(svc, &req.account_id, &req.region, &named);
    {
        let mut accounts = svc.state.write();
        accounts
            .regional_mut(&req.account_id, &req.region)
            .set_ebs_default_kms_key(&req.region, Some(key.clone()));
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
            .regional_mut(&req.account_id, &req.region)
            .set_ebs_default_kms_key(&req.region, None);
    }
    let key = ebs_default_key(svc, &req.account_id, &req.region)
        .unwrap_or_else(|| "alias/aws/ebs".to_string());
    Ok(Ec2Service::respond(
        "ResetEbsDefaultKmsKeyId",
        &req.request_id,
        &ec2_elem("kmsKeyId", &key),
    ))
}

// ---- recycle bin ----

pub(crate) fn list_volumes_in_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
        let st = accounts.regional_mut("000000000000", "us-east-1");
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
    fn create_volume_honors_iops_throughput_multiattach() {
        // bug-audit 2026-07-27 (cycle 6): create hardcoded iops=3000/throughput=125/
        // multi_attach=false, ignoring the request -> aws_ebs_volume perpetual drift.
        let svc = Ec2Service::new();
        let body = body_of(
            create_volume(
                &svc,
                &req(
                    "CreateVolume",
                    &[
                        ("AvailabilityZone", "us-east-1a"),
                        ("Size", "100"),
                        ("VolumeType", "gp3"),
                        ("Iops", "6000"),
                        ("Throughput", "250"),
                        ("MultiAttachEnabled", "true"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(body.contains("<iops>6000</iops>"), "{body}");
        assert!(body.contains("<throughput>250</throughput>"), "{body}");
        assert!(
            body.contains("<multiAttachEnabled>true</multiAttachEnabled>"),
            "{body}"
        );

        // A standard (magnetic) volume has neither iops nor throughput.
        let body2 = body_of(
            create_volume(
                &svc,
                &req(
                    "CreateVolume",
                    &[
                        ("AvailabilityZone", "us-east-1a"),
                        ("Size", "500"),
                        ("VolumeType", "standard"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(!body2.contains("<throughput>"), "{body2}");
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
            .regional("000000000000", "us-east-1")
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
            .regional("000000000000", "us-east-1")
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
            .regional_mut("000000000000", "us-east-1")
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
            let v = &st.regional("000000000000", "us-east-1").unwrap().volumes["vol-1"];
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
            let st = accounts.regional_mut("000000000000", "us-east-1");
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

    /// The text of the first `<tag>` element in `xml`, if any.
    fn elem(xml: &str, tag: &str) -> Option<String> {
        let open = format!("<{tag}>");
        let start = xml.find(&open)? + open.len();
        let end = xml[start..].find(&format!("</{tag}>"))? + start;
        Some(xml[start..end].to_string())
    }

    fn kms_svc() -> (fakecloud_kms::SharedKmsState, Ec2Service) {
        let (kms, hook) = fakecloud_kms::test_support::kms_hook("000000000000");
        (kms, Ec2Service::new().with_kms_hook(Some(hook)))
    }

    fn assert_ebs_key(kms: &fakecloud_kms::SharedKmsState, arn: &str) {
        fakecloud_kms::test_support::assert_aws_managed_key(
            kms,
            "000000000000",
            "us-east-1",
            arn,
            "alias/aws/ebs",
        );
    }

    /// An encrypted volume with no key named reports the region's AWS-managed
    /// `aws/ebs` key (a real KMS key), which is also what
    /// GetEbsDefaultKmsKeyId reports; an unencrypted one reports no key.
    #[test]
    fn encrypted_volume_reports_the_aws_managed_ebs_key() {
        let (kms, svc) = kms_svc();
        let out = body_of(
            create_volume(
                &svc,
                &req(
                    "CreateVolume",
                    &[("AvailabilityZone", "us-east-1a"), ("Encrypted", "true")],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains("<encrypted>true</encrypted>"), "{out}");
        let key = elem(&out, "kmsKeyId").expect("encrypted volume reports a key");
        assert_ebs_key(&kms, &key);
        let default =
            body_of(get_ebs_default_kms_key_id(&svc, &req("GetEbsDefaultKmsKeyId", &[])).unwrap());
        assert_eq!(elem(&default, "kmsKeyId").as_deref(), Some(key.as_str()));

        let plain = body_of(
            create_volume(
                &svc,
                &req("CreateVolume", &[("AvailabilityZone", "us-east-1a")]),
            )
            .unwrap(),
        );
        assert!(plain.contains("<encrypted>false</encrypted>"), "{plain}");
        assert_eq!(elem(&plain, "kmsKeyId"), None);

        // A named alias is reported as its key's ARN.
        let named = body_of(
            create_volume(
                &svc,
                &req(
                    "CreateVolume",
                    &[
                        ("AvailabilityZone", "us-east-1a"),
                        ("Encrypted", "true"),
                        ("KmsKeyId", "alias/aws/ebs"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(elem(&named, "kmsKeyId").as_deref(), Some(key.as_str()));
    }

    /// Encryption by default encrypts every new volume with the account's EBS
    /// default key: the AWS-managed key, or the key ModifyEbsDefaultKmsKeyId
    /// set, until ResetEbsDefaultKmsKeyId restores the AWS-managed one.
    #[test]
    fn encryption_by_default_uses_the_account_default_key() {
        let (kms, svc) = kms_svc();
        enable_ebs_encryption_by_default(&svc, &req("EnableEbsEncryptionByDefault", &[])).unwrap();
        let out = body_of(
            create_volume(
                &svc,
                &req("CreateVolume", &[("AvailabilityZone", "us-east-1a")]),
            )
            .unwrap(),
        );
        assert!(out.contains("<encrypted>true</encrypted>"), "{out}");
        let managed = elem(&out, "kmsKeyId").expect("default-encrypted volume reports a key");
        assert_ebs_key(&kms, &managed);

        let custom = "arn:aws:kms:us-east-1:000000000000:key/custom-default";
        let modified = body_of(
            modify_ebs_default_kms_key_id(
                &svc,
                &req("ModifyEbsDefaultKmsKeyId", &[("KmsKeyId", custom)]),
            )
            .unwrap(),
        );
        assert_eq!(elem(&modified, "kmsKeyId").as_deref(), Some(custom));
        let got =
            body_of(get_ebs_default_kms_key_id(&svc, &req("GetEbsDefaultKmsKeyId", &[])).unwrap());
        assert_eq!(elem(&got, "kmsKeyId").as_deref(), Some(custom));
        let out = body_of(
            create_volume(
                &svc,
                &req("CreateVolume", &[("AvailabilityZone", "us-east-1a")]),
            )
            .unwrap(),
        );
        assert_eq!(elem(&out, "kmsKeyId").as_deref(), Some(custom));

        // An alias names its key: ModifyEbsDefaultKmsKeyId stores and reports
        // the key ARN.
        let by_alias = body_of(
            modify_ebs_default_kms_key_id(
                &svc,
                &req("ModifyEbsDefaultKmsKeyId", &[("KmsKeyId", "alias/aws/ebs")]),
            )
            .unwrap(),
        );
        assert_eq!(
            elem(&by_alias, "kmsKeyId").as_deref(),
            Some(managed.as_str())
        );

        let reset = body_of(
            reset_ebs_default_kms_key_id(&svc, &req("ResetEbsDefaultKmsKeyId", &[])).unwrap(),
        );
        assert_eq!(elem(&reset, "kmsKeyId").as_deref(), Some(managed.as_str()));

        disable_ebs_encryption_by_default(&svc, &req("DisableEbsEncryptionByDefault", &[]))
            .unwrap();
        let out = body_of(
            create_volume(
                &svc,
                &req("CreateVolume", &[("AvailabilityZone", "us-east-1a")]),
            )
            .unwrap(),
        );
        assert!(out.contains("<encrypted>false</encrypted>"), "{out}");
    }

    /// Snapshots carry their volume's key; a volume restored from an
    /// encrypted snapshot inherits its encryption, key and size; a copy keeps
    /// the source key in-region, and a newly encrypted copy uses the default
    /// key.
    #[test]
    fn snapshots_and_restores_carry_the_key() {
        use crate::service::snapshot::{copy_snapshot, create_snapshot, describe_snapshots};
        let (kms, svc) = kms_svc();
        let vol = body_of(
            create_volume(
                &svc,
                &req(
                    "CreateVolume",
                    &[
                        ("AvailabilityZone", "us-east-1a"),
                        ("Encrypted", "true"),
                        ("Size", "42"),
                    ],
                ),
            )
            .unwrap(),
        );
        let vol_id = elem(&vol, "volumeId").unwrap();
        let key = elem(&vol, "kmsKeyId").unwrap();
        assert_ebs_key(&kms, &key);

        let snap = body_of(
            create_snapshot(&svc, &req("CreateSnapshot", &[("VolumeId", &vol_id)])).unwrap(),
        );
        let snap_id = elem(&snap, "snapshotId").unwrap();
        assert!(snap.contains("<encrypted>true</encrypted>"), "{snap}");
        assert_eq!(elem(&snap, "kmsKeyId").as_deref(), Some(key.as_str()));
        let described = body_of(
            describe_snapshots(
                &svc,
                &req("DescribeSnapshots", &[("SnapshotId.1", &snap_id)]),
            )
            .unwrap(),
        );
        assert_eq!(elem(&described, "kmsKeyId").as_deref(), Some(key.as_str()));

        let restored = body_of(
            create_volume(
                &svc,
                &req(
                    "CreateVolume",
                    &[("AvailabilityZone", "us-east-1a"), ("SnapshotId", &snap_id)],
                ),
            )
            .unwrap(),
        );
        assert!(
            restored.contains("<encrypted>true</encrypted>"),
            "{restored}"
        );
        assert!(restored.contains("<size>42</size>"), "{restored}");
        assert_eq!(elem(&restored, "kmsKeyId").as_deref(), Some(key.as_str()));

        let copy = body_of(
            copy_snapshot(
                &svc,
                &req(
                    "CopySnapshot",
                    &[
                        ("SourceRegion", "us-east-1"),
                        ("SourceSnapshotId", &snap_id),
                    ],
                ),
            )
            .unwrap(),
        );
        let copy_id = elem(&copy, "snapshotId").unwrap();
        let described = body_of(
            describe_snapshots(
                &svc,
                &req("DescribeSnapshots", &[("SnapshotId.1", &copy_id)]),
            )
            .unwrap(),
        );
        assert!(
            described.contains("<encrypted>true</encrypted>"),
            "{described}"
        );
        assert!(
            described.contains("<volumeSize>42</volumeSize>"),
            "{described}"
        );
        assert_eq!(elem(&described, "kmsKeyId").as_deref(), Some(key.as_str()));

        // Encrypting a copy of an unencrypted snapshot uses the default key.
        let plain = body_of(
            create_volume(
                &svc,
                &req("CreateVolume", &[("AvailabilityZone", "us-east-1a")]),
            )
            .unwrap(),
        );
        let plain_snap = body_of(
            create_snapshot(
                &svc,
                &req(
                    "CreateSnapshot",
                    &[("VolumeId", &elem(&plain, "volumeId").unwrap())],
                ),
            )
            .unwrap(),
        );
        assert_eq!(elem(&plain_snap, "kmsKeyId"), None);
        let encrypted_copy = body_of(
            copy_snapshot(
                &svc,
                &req(
                    "CopySnapshot",
                    &[
                        ("SourceRegion", "us-east-1"),
                        (
                            "SourceSnapshotId",
                            &elem(&plain_snap, "snapshotId").unwrap(),
                        ),
                        ("Encrypted", "true"),
                    ],
                ),
            )
            .unwrap(),
        );
        let described = body_of(
            describe_snapshots(
                &svc,
                &req(
                    "DescribeSnapshots",
                    &[(
                        "SnapshotId.1",
                        &elem(&encrypted_copy, "snapshotId").unwrap(),
                    )],
                ),
            )
            .unwrap(),
        );
        assert!(
            described.contains("<encrypted>true</encrypted>"),
            "{described}"
        );
        assert_eq!(elem(&described, "kmsKeyId").as_deref(), Some(key.as_str()));
    }

    /// RunInstances creates the EBS volumes its block-device mappings name,
    /// attached and encrypted as asked (the default key when none is named);
    /// DescribeInstances reports them, and terminating the instance deletes
    /// the DeleteOnTermination ones and detaches the rest.
    #[tokio::test]
    async fn run_instances_creates_encrypted_block_device_volumes() {
        use crate::service::instance::{describe_instances, run_instances, terminate_instances};
        let (kms, svc) = kms_svc();
        let out = body_of(
            run_instances(
                &svc,
                &req(
                    "RunInstances",
                    &[
                        ("ImageId", "ami-12345678"),
                        ("MinCount", "1"),
                        ("MaxCount", "1"),
                        ("BlockDeviceMapping.1.DeviceName", "/dev/xvda"),
                        ("BlockDeviceMapping.1.Ebs.VolumeSize", "20"),
                        ("BlockDeviceMapping.1.Ebs.Encrypted", "true"),
                        ("BlockDeviceMapping.2.DeviceName", "/dev/sdf"),
                        ("BlockDeviceMapping.2.Ebs.VolumeSize", "5"),
                        ("BlockDeviceMapping.2.Ebs.DeleteOnTermination", "false"),
                        ("BlockDeviceMapping.3.DeviceName", "/dev/sdg"),
                        ("BlockDeviceMapping.3.NoDevice", ""),
                    ],
                ),
            )
            .await
            .unwrap(),
        );
        let instance_id = elem(&out, "instanceId").unwrap();
        assert!(out.contains("<deviceName>/dev/xvda</deviceName>"), "{out}");
        assert!(out.contains("<deviceName>/dev/sdf</deviceName>"), "{out}");
        assert!(!out.contains("/dev/sdg"), "{out}");

        let (root, data) = {
            let accounts = svc.state.read();
            let st = accounts.regional("000000000000", "us-east-1").unwrap();
            let find = |dev: &str| {
                st.volumes
                    .values()
                    .find(|v| v.attachments.iter().any(|a| a.device == dev))
                    .cloned()
                    .unwrap()
            };
            (find("/dev/xvda"), find("/dev/sdf"))
        };
        assert_eq!(root.size, 20);
        assert_eq!(root.state, "in-use");
        assert!(root.encrypted);
        assert_ebs_key(&kms, root.kms_key_id.as_deref().unwrap());
        assert!(!data.encrypted);
        assert_eq!(data.kms_key_id, None);

        let described = body_of(
            describe_instances(
                &svc,
                &req("DescribeInstances", &[("InstanceId.1", &instance_id)]),
            )
            .unwrap(),
        );
        assert!(
            described.contains(&format!("<volumeId>{}</volumeId>", root.volume_id)),
            "{described}"
        );

        terminate_instances(
            &svc,
            &req("TerminateInstances", &[("InstanceId.1", &instance_id)]),
        )
        .await
        .unwrap();
        let accounts = svc.state.read();
        let st = accounts.regional("000000000000", "us-east-1").unwrap();
        assert!(!st.volumes.contains_key(&root.volume_id));
        let kept = &st.volumes[&data.volume_id];
        assert_eq!(kept.state, "available");
        assert!(kept.attachments.is_empty());
    }

    /// EBS encryption by default and the EBS default key are per region: a
    /// customization in one region leaves every other region on its own
    /// AWS-managed `aws/ebs` key with encryption by default off.
    #[test]
    fn ebs_defaults_are_per_region() {
        let (kms, svc) = kms_svc();
        let in_region = |action: &str, query: &[(&str, &str)], region: &str| {
            let mut r = req(action, query);
            r.region = region.to_string();
            r
        };
        let east_key = elem(
            &body_of(
                modify_ebs_default_kms_key_id(
                    &svc,
                    &in_region(
                        "ModifyEbsDefaultKmsKeyId",
                        &[("KmsKeyId", "alias/aws/ebs")],
                        "us-east-1",
                    ),
                )
                .unwrap(),
            ),
            "kmsKeyId",
        )
        .unwrap();
        enable_ebs_encryption_by_default(
            &svc,
            &in_region("EnableEbsEncryptionByDefault", &[], "us-east-1"),
        )
        .unwrap();

        let west_default = elem(
            &body_of(
                get_ebs_default_kms_key_id(
                    &svc,
                    &in_region("GetEbsDefaultKmsKeyId", &[], "us-west-2"),
                )
                .unwrap(),
            ),
            "kmsKeyId",
        )
        .unwrap();
        assert_ne!(west_default, east_key);
        fakecloud_kms::test_support::assert_aws_managed_key(
            &kms,
            "000000000000",
            "us-west-2",
            &west_default,
            "alias/aws/ebs",
        );
        let west_plain = body_of(
            create_volume(
                &svc,
                &in_region(
                    "CreateVolume",
                    &[("AvailabilityZone", "us-west-2a")],
                    "us-west-2",
                ),
            )
            .unwrap(),
        );
        assert!(
            west_plain.contains("<encrypted>false</encrypted>"),
            "{west_plain}"
        );
        let west_enc = body_of(
            create_volume(
                &svc,
                &in_region(
                    "CreateVolume",
                    &[("AvailabilityZone", "us-west-2a"), ("Encrypted", "true")],
                    "us-west-2",
                ),
            )
            .unwrap(),
        );
        assert_eq!(
            elem(&west_enc, "kmsKeyId").as_deref(),
            Some(west_default.as_str())
        );
        let east_vol = body_of(
            create_volume(
                &svc,
                &in_region(
                    "CreateVolume",
                    &[("AvailabilityZone", "us-east-1a")],
                    "us-east-1",
                ),
            )
            .unwrap(),
        );
        assert_eq!(
            elem(&east_vol, "kmsKeyId").as_deref(),
            Some(east_key.as_str())
        );
    }

    /// A default key persisted before the setting was per region keeps
    /// applying to the region its ARN names, and only there.
    #[test]
    fn legacy_ebs_default_key_applies_to_its_own_region() {
        let mut st = Ec2State::new("000000000000", "us-east-1");
        st.ebs_default_kms_key_id =
            Some("arn:aws:kms:us-east-1:000000000000:key/legacy".to_string());
        assert_eq!(
            st.ebs_default_kms_key("us-east-1").as_deref(),
            Some("arn:aws:kms:us-east-1:000000000000:key/legacy")
        );
        assert_eq!(st.ebs_default_kms_key("us-west-2"), None);
        st.set_ebs_default_kms_key("us-east-1", None);
        assert_eq!(st.ebs_default_kms_key("us-east-1"), None);

        // A legacy key that names no region covered every region; changing
        // or resetting one region leaves the others on it.
        st.ebs_default_kms_key_id = Some("alias/my-key".to_string());
        st.set_ebs_default_kms_key(
            "us-west-2",
            Some("arn:aws:kms:us-west-2:000000000000:key/west".to_string()),
        );
        st.set_ebs_default_kms_key("eu-west-1", None);
        assert_eq!(
            st.ebs_default_kms_key("us-west-2").as_deref(),
            Some("arn:aws:kms:us-west-2:000000000000:key/west")
        );
        assert_eq!(st.ebs_default_kms_key("eu-west-1"), None);
        assert_eq!(
            st.ebs_default_kms_key("ap-south-1").as_deref(),
            Some("alias/my-key")
        );
    }
}
