//! EBS snapshots: lifecycle, attributes, tiering, locking, recycle bin,
//! fast-snapshot-restore, and snapshot block-public-access.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, require_struct, validate_enum,
    validate_int_range, validate_max_results, Filter,
};
use crate::state::{Ec2State, Snapshot, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn snapshot_xml(s: &Snapshot, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<volumeSize>{}</volumeSize>{}{}<encrypted>{}</encrypted>{}{}{}{}",
        ec2_elem("snapshotId", &s.snapshot_id),
        ec2_elem("volumeId", &s.volume_id),
        ec2_elem("status", &s.state),
        s.volume_size,
        ec2_elem("startTime", FIXED_TIME),
        ec2_elem("progress", "100%"),
        s.encrypted,
        ec2_elem("ownerId", owner),
        ec2_elem("description", &s.description),
        ec2_elem("storageTier", &s.storage_tier),
        super::tags::tag_set_xml(tags),
    )
}

fn build_snapshot(volume_id: String, description: String) -> Snapshot {
    Snapshot {
        snapshot_id: gen_id("snap"),
        volume_id,
        state: "completed".to_string(),
        volume_size: 8,
        description,
        encrypted: false,
        storage_tier: "standard".to_string(),
        in_recycle_bin: false,
        locked: false,
        lock_mode: None,
    }
}

pub(crate) fn create_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let volume_id = require(&req.query_params, "VolumeId")?;
    validate_enum(&req.query_params, "Location", &["regional", "local"])?;
    let snap = build_snapshot(
        volume_id,
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    );
    let id = snap.snapshot_id.clone();
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "snapshot");
        let t = state.tags_for(&id).to_vec();
        state.snapshots.insert(id.clone(), snap.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateSnapshot",
        &req.request_id,
        &snapshot_xml(&snap, &tags, &owner),
    ))
}

pub(crate) fn create_snapshots(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "InstanceSpecification")?;
    validate_enum(&req.query_params, "Location", &["regional", "local"])?;
    validate_enum(&req.query_params, "CopyTagsFromSource", &["volume"])?;
    let owner = req.account_id.clone();
    let snap = build_snapshot("vol-0123456789abcdef0".to_string(), String::new());
    let id = snap.snapshot_id.clone();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .snapshots
            .insert(id.clone(), snap.clone());
    }
    let item = snapshot_xml(&snap, &[], &owner);
    Ok(Ec2Service::respond(
        "CreateSnapshots",
        &req.request_id,
        &ec2_list("snapshotSet", &[item]),
    ))
}

pub(crate) fn delete_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.snapshots.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteSnapshot",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_snapshots(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "SnapshotId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .snapshots
        .values()
        .filter(|s| !s.in_recycle_bin)
        .filter(|s| wanted.is_empty() || wanted.contains(&s.snapshot_id))
        .filter(|s| snap_match(s, state.tags_for(&s.snapshot_id), &filters))
        .map(|s| snapshot_xml(s, state.tags_for(&s.snapshot_id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeSnapshots",
        &req.request_id,
        &ec2_list("snapshotSet", &items),
    ))
}

fn snap_match(s: &Snapshot, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "snapshot-id" => vec![s.snapshot_id.clone()],
            "volume-id" => vec![s.volume_id.clone()],
            "status" => vec![s.state.clone()],
            "storage-tier" => vec![s.storage_tier.clone()],
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

pub(crate) fn copy_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SourceRegion")?;
    let src = require(&req.query_params, "SourceSnapshotId")?;
    validate_int_range(&req.query_params, "CompletionDurationMinutes", 1, 2880)?;
    let snap = build_snapshot(
        format!("vol-copy-{src}"),
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    );
    let id = snap.snapshot_id.clone();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .snapshots
            .insert(id.clone(), snap);
    }
    Ok(Ec2Service::respond(
        "CopySnapshot",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("snapshotId", &id),
            super::tags::tag_set_xml(&[])
        ),
    ))
}

pub(crate) fn describe_snapshot_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    let attr = require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["productCodes", "createVolumePermission"],
    )?;
    let attr_xml = match attr.as_str() {
        "productCodes" => ec2_list("productCodes", &[]),
        _ => ec2_list("createVolumePermission", &[]),
    };
    Ok(Ec2Service::respond(
        "DescribeSnapshotAttribute",
        &req.request_id,
        &format!("{}{}", ec2_elem("snapshotId", &id), attr_xml),
    ))
}

pub(crate) fn modify_snapshot_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SnapshotId")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["productCodes", "createVolumePermission"],
    )?;
    validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
    Ok(Ec2Service::respond(
        "ModifySnapshotAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_snapshot_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SnapshotId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["productCodes", "createVolumePermission"],
    )?;
    Ok(Ec2Service::respond(
        "ResetSnapshotAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn modify_snapshot_tier(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    validate_enum(&req.query_params, "StorageTier", &["archive"])?;
    {
        let mut accounts = svc.state.write();
        if let Some(s) = accounts
            .get_or_create(&req.account_id)
            .snapshots
            .get_mut(&id)
        {
            s.storage_tier = "archive".to_string();
        }
    }
    Ok(Ec2Service::respond(
        "ModifySnapshotTier",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("snapshotId", &id),
            ec2_elem("tieringStartTime", FIXED_TIME)
        ),
    ))
}

pub(crate) fn describe_snapshot_tier_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .snapshots
        .values()
        .map(|s| {
            format!(
                "{}{}{}",
                ec2_elem("snapshotId", &s.snapshot_id),
                ec2_elem("volumeId", &s.volume_id),
                ec2_elem("ownerId", &owner)
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeSnapshotTierStatus",
        &req.request_id,
        &ec2_list("snapshotTierStatusSet", &items),
    ))
}

pub(crate) fn restore_snapshot_tier(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(s) = accounts
            .get_or_create(&req.account_id)
            .snapshots
            .get_mut(&id)
        {
            s.storage_tier = "standard".to_string();
        }
    }
    Ok(Ec2Service::respond(
        "RestoreSnapshotTier",
        &req.request_id,
        &format!(
            "{}{}<isPermanentRestore>false</isPermanentRestore>",
            ec2_elem("snapshotId", &id),
            ec2_elem("restoreStartTime", FIXED_TIME)
        ),
    ))
}

pub(crate) fn list_snapshots_in_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .snapshots
        .values()
        .filter(|s| s.in_recycle_bin)
        .map(|s| {
            format!(
                "{}{}",
                ec2_elem("snapshotId", &s.snapshot_id),
                ec2_elem("description", &s.description)
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "ListSnapshotsInRecycleBin",
        &req.request_id,
        &ec2_list("snapshotSet", &items),
    ))
}

pub(crate) fn restore_snapshot_from_recycle_bin(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(s) = accounts
            .get_or_create(&req.account_id)
            .snapshots
            .get_mut(&id)
        {
            s.in_recycle_bin = false;
        }
    }
    Ok(Ec2Service::respond(
        "RestoreSnapshotFromRecycleBin",
        &req.request_id,
        &format!("{}{}", ec2_elem("snapshotId", &id), ec2_return(true)),
    ))
}

pub(crate) fn lock_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    let mode = require(&req.query_params, "LockMode")?;
    validate_enum(&req.query_params, "LockMode", &["compliance", "governance"])?;
    validate_int_range(&req.query_params, "CoolOffPeriod", 1, 72)?;
    validate_int_range(&req.query_params, "LockDuration", 1, 36500)?;
    {
        let mut accounts = svc.state.write();
        if let Some(s) = accounts
            .get_or_create(&req.account_id)
            .snapshots
            .get_mut(&id)
        {
            s.locked = true;
            s.lock_mode = Some(mode.clone());
        }
    }
    let state = if mode == "governance" {
        "governance"
    } else {
        "compliance-cooloff"
    };
    Ok(Ec2Service::respond(
        "LockSnapshot",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("snapshotId", &id),
            ec2_elem("lockState", state)
        ),
    ))
}

pub(crate) fn unlock_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(s) = accounts
            .get_or_create(&req.account_id)
            .snapshots
            .get_mut(&id)
        {
            s.locked = false;
            s.lock_mode = None;
        }
    }
    Ok(Ec2Service::respond(
        "UnlockSnapshot",
        &req.request_id,
        &ec2_elem("snapshotId", &id),
    ))
}

pub(crate) fn describe_locked_snapshots(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .snapshots
        .values()
        .filter(|s| s.locked)
        .map(|s| {
            format!(
                "{}{}{}",
                ec2_elem("snapshotId", &s.snapshot_id),
                ec2_elem("ownerId", &owner),
                ec2_elem("lockState", s.lock_mode.as_deref().unwrap_or("governance")),
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeLockedSnapshots",
        &req.request_id,
        &ec2_list("snapshotSet", &items),
    ))
}

// ---- block public access (account-level) ----

pub(crate) fn get_snapshot_block_public_access_state(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let state = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .map(|s| s.snapshot_block_public_access.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unblocked".to_string())
    };
    Ok(Ec2Service::respond(
        "GetSnapshotBlockPublicAccessState",
        &req.request_id,
        &ec2_elem("state", &state),
    ))
}

pub(crate) fn enable_snapshot_block_public_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let val = require(&req.query_params, "State")?;
    validate_enum(
        &req.query_params,
        "State",
        &["block-all-sharing", "block-new-sharing", "unblocked"],
    )?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .snapshot_block_public_access = val.clone();
    }
    Ok(Ec2Service::respond(
        "EnableSnapshotBlockPublicAccess",
        &req.request_id,
        &ec2_elem("state", &val),
    ))
}

pub(crate) fn disable_snapshot_block_public_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .snapshot_block_public_access = "unblocked".to_string();
    }
    Ok(Ec2Service::respond(
        "DisableSnapshotBlockPublicAccess",
        &req.request_id,
        &ec2_elem("state", "unblocked"),
    ))
}

// ---- fast snapshot restores ----

fn fsr_set(req: &AwsRequest, action: &str, state: &str) -> Result<AwsResponse, AwsServiceError> {
    let snaps = indexed_list(&req.query_params, "SourceSnapshotId");
    let azs = indexed_list(&req.query_params, "AvailabilityZone");
    let az = azs
        .first()
        .cloned()
        .unwrap_or_else(|| "us-east-1a".to_string());
    let items: Vec<String> = snaps
        .iter()
        .map(|s| {
            format!(
                "{}{}{}",
                ec2_elem("snapshotId", s),
                ec2_elem("availabilityZone", &az),
                ec2_elem("state", state)
            )
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successful", &items),
        ec2_list("unsuccessful", &[])
    );
    Ok(Ec2Service::respond(action, &req.request_id, &body))
}

pub(crate) fn enable_fast_snapshot_restores(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    fsr_set(req, "EnableFastSnapshotRestores", "enabling")
}
pub(crate) fn disable_fast_snapshot_restores(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    fsr_set(req, "DisableFastSnapshotRestores", "disabling")
}

pub(crate) fn describe_fast_snapshot_restores(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 0, 200)?;
    Ok(Ec2Service::respond(
        "DescribeFastSnapshotRestores",
        &req.request_id,
        &ec2_list("fastSnapshotRestoreSet", &[]),
    ))
}
