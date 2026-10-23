//! EBS snapshots: lifecycle, attributes, tiering, locking, recycle bin,
//! fast-snapshot-restore, and snapshot block-public-access.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, invalid_parameter_value, not_found, paginate,
    parse_filters, require, require_struct, validate_enum, validate_int_range,
    validate_max_results, Filter,
};
use crate::state::{Ec2State, Snapshot, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn snapshot_xml(s: &Snapshot, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<volumeSize>{}</volumeSize>{}{}<encrypted>{}</encrypted>{}{}{}{}{}",
        ec2_elem("snapshotId", &s.snapshot_id),
        ec2_elem("volumeId", &s.volume_id),
        ec2_elem("status", &s.state),
        s.volume_size,
        ec2_elem("startTime", FIXED_TIME),
        ec2_elem("progress", "100%"),
        s.encrypted,
        s.kms_key_id
            .as_deref()
            .map(|k| ec2_elem("kmsKeyId", k))
            .unwrap_or_default(),
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
        kms_key_id: None,
        storage_tier: "standard".to_string(),
        in_recycle_bin: false,
        locked: false,
        lock_mode: None,
        create_volume_permissions: Vec::new(),
    }
}

pub(crate) fn create_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let volume_id = require(&req.query_params, "VolumeId")?;
    validate_enum(&req.query_params, "Location", &["regional", "local"])?;
    let mut snap = build_snapshot(
        volume_id.clone(),
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    );
    let id = snap.snapshot_id.clone();
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        // The source volume must exist — AWS rejects a snapshot of a phantom id.
        let vol = state
            .volumes
            .get(&volume_id)
            .ok_or_else(|| not_found("InvalidVolume.NotFound", &volume_id))?;
        snap.volume_size = vol.size;
        // A snapshot of an encrypted volume is encrypted with its key.
        snap.encrypted = vol.encrypted;
        snap.kms_key_id = vol.kms_key_id.clone().filter(|_| vol.encrypted);
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
    let instance_id = require(&req.query_params, "InstanceSpecification.InstanceId")?;
    let description = req
        .query_params
        .get("Description")
        .cloned()
        .unwrap_or_default();
    let items: Vec<String> = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        if !state.instances.contains_key(&instance_id) {
            return Err(not_found("InvalidInstanceID.NotFound", &instance_id));
        }
        // One snapshot per volume attached to the instance, each carrying the
        // real source volume id and size (not a fabricated placeholder).
        let sources: Vec<(String, i64, bool, Option<String>)> = state
            .volumes
            .values()
            .filter(|v| v.attachments.iter().any(|a| a.instance_id == instance_id))
            .map(|v| {
                let key = v.kms_key_id.clone().filter(|_| v.encrypted);
                (v.volume_id.clone(), v.size, v.encrypted, key)
            })
            .collect();
        // `CopyTagsFromSource=volume` copies each source volume's tags onto
        // its snapshot; `TagSpecification` tags are applied on top.
        let copy_volume_tags = req
            .query_params
            .get("CopyTagsFromSource")
            .is_some_and(|v| v == "volume");
        let mut rendered = Vec::new();
        for (vol_id, size, encrypted, kms_key_id) in sources {
            let volume_tags = if copy_volume_tags {
                state.tags_for(&vol_id).to_vec()
            } else {
                Vec::new()
            };
            let mut snap = build_snapshot(vol_id, description.clone());
            snap.volume_size = size;
            snap.encrypted = encrypted;
            snap.kms_key_id = kms_key_id;
            let id = snap.snapshot_id.clone();
            if !volume_tags.is_empty() {
                state.upsert_tags(&id, &volume_tags);
            }
            crate::service::tags::apply_tag_specifications(
                state,
                &req.query_params,
                &id,
                "snapshot",
            );
            rendered.push(snapshot_xml(&snap, state.tags_for(&id), &owner));
            state.snapshots.insert(id, snap);
        }
        rendered
    };
    Ok(Ec2Service::respond(
        "CreateSnapshots",
        &req.request_id,
        &ec2_list("snapshotSet", &items),
    ))
}

pub(crate) fn delete_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if state.snapshots.remove(&id).is_none() {
        return Err(not_found("InvalidSnapshot.NotFound", &id));
    }
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
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "SnapshotId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);

    // An explicitly-requested SnapshotId that does not exist (or is in the
    // recycle bin) is a hard error on AWS (InvalidSnapshot.NotFound), not a
    // silently-empty result. Matches the sibling Describe ops.
    for id in &wanted {
        if state.snapshots.get(id).is_none_or(|s| s.in_recycle_bin) {
            return Err(not_found("InvalidSnapshot.NotFound", id));
        }
    }

    let mut items: Vec<String> = state
        .snapshots
        .values()
        .filter(|s| !s.in_recycle_bin)
        .filter(|s| wanted.is_empty() || wanted.contains(&s.snapshot_id))
        .filter(|s| snap_match(s, state.tags_for(&s.snapshot_id), &filters, &owner))
        .map(|s| snapshot_xml(s, state.tags_for(&s.snapshot_id), &owner))
        .collect();
    items.sort();

    // DescribeSnapshots is `paginated` in the model; honor MaxResults +
    // NextToken instead of always returning the full set.
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = paginate(&items, next_token, max_results)?;
    let body = format!(
        "{}{}",
        ec2_list("snapshotSet", &page),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond(
        "DescribeSnapshots",
        &req.request_id,
        &body,
    ))
}

fn snap_match(s: &Snapshot, tags: &[Tag], filters: &[Filter], owner: &str) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "snapshot-id" => vec![s.snapshot_id.clone()],
            "volume-id" => vec![s.volume_id.clone()],
            "status" => vec![s.state.clone()],
            "storage-tier" => vec![s.storage_tier.clone()],
            "owner-id" => vec![owner.to_string()],
            // Self-owned snapshots have no owner alias on AWS.
            "owner-alias" => vec![String::new()],
            // Snapshots are rendered as fully complete (see `snapshot_xml`).
            "progress" => vec!["100%".to_string()],
            "start-time" => vec![FIXED_TIME.to_string()],
            "encrypted" => vec![s.encrypted.to_string()],
            "volume-size" => vec![s.volume_size.to_string()],
            "description" => vec![s.description.clone()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
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
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

pub(crate) fn copy_snapshot(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let source_region = require(&req.query_params, "SourceRegion")?;
    let src = require(&req.query_params, "SourceSnapshotId")?;
    validate_int_range(&req.query_params, "CompletionDurationMinutes", 1, 2880)?;
    let mut snap = build_snapshot(
        format!("vol-copy-{src}"),
        req.query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    );
    let source = {
        let accounts = svc.state.read();
        accounts
            .regional(&req.account_id, &req.region)
            .and_then(|s| s.snapshots.get(&src))
            .map(|s| (s.volume_size, s.encrypted, s.kms_key_id.clone()))
    };
    let (source_encrypted, source_key) = match source {
        Some((size, encrypted, key)) => {
            snap.volume_size = size;
            (encrypted, key)
        }
        None => (false, None),
    };
    // A copy is encrypted when asked or when its source is. It uses the named
    // key (as its ARN); otherwise a same-region copy of an encrypted snapshot
    // keeps the source's key, and any other encrypted copy (a newly encrypted
    // one, or a cross-region one, whose source key lives in another region)
    // uses the account's EBS default key.
    let requested = req
        .query_params
        .get("Encrypted")
        .is_some_and(|v| v == "true");
    if requested || source_encrypted {
        snap.encrypted = true;
        let named = req.query_params.get("KmsKeyId").filter(|k| !k.is_empty());
        snap.kms_key_id = match named {
            Some(key) => Some(super::volume::named_ebs_key(
                svc,
                &req.account_id,
                &req.region,
                key,
            )),
            None if source_encrypted && source_region == req.region => source_key
                .or_else(|| super::volume::ebs_default_key(svc, &req.account_id, &req.region)),
            None => super::volume::ebs_default_key(svc, &req.account_id, &req.region),
        };
    }
    let id = snap.snapshot_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "snapshot");
        state.snapshots.insert(id.clone(), snap);
        state.tags_for(&id).to_vec()
    };
    Ok(Ec2Service::respond(
        "CopySnapshot",
        &req.request_id,
        &format!(
            "{}{}",
            ec2_elem("snapshotId", &id),
            super::tags::tag_set_xml(&tags)
        ),
    ))
}

pub(crate) fn describe_snapshot_attribute(
    svc: &Ec2Service,
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
        _ => {
            let accounts = svc.state.read();
            let perms = accounts
                .regional(&req.account_id, &req.region)
                .and_then(|s| s.snapshots.get(&id))
                .map(|s| s.create_volume_permissions.clone())
                .unwrap_or_default();
            let items: Vec<String> = perms
                .iter()
                .map(|g| {
                    if g == "all" {
                        ec2_elem("group", "all")
                    } else {
                        ec2_elem("userId", g)
                    }
                })
                .collect();
            ec2_list("createVolumePermission", &items)
        }
    };
    Ok(Ec2Service::respond(
        "DescribeSnapshotAttribute",
        &req.request_id,
        &format!("{}{}", ec2_elem("snapshotId", &id), attr_xml),
    ))
}

pub(crate) fn modify_snapshot_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["productCodes", "createVolumePermission"],
    )?;
    validate_enum(&req.query_params, "OperationType", &["add", "remove"])?;
    let p = &req.query_params;
    // The only volume-permission group AWS recognizes is `all` (public).
    let validate_group = |g: &str| -> Result<(), AwsServiceError> {
        if g == "all" {
            Ok(())
        } else {
            Err(invalid_parameter_value(format!(
                "Invalid group '{g}' for createVolumePermission"
            )))
        }
    };
    // Collect grantees from both the structured Add/Remove lists and the
    // legacy OperationType + UserId.N / UserGroup.N form.
    let collect = |kind: &str| -> Result<Vec<String>, AwsServiceError> {
        let mut out = Vec::new();
        let mut n = 1usize;
        loop {
            let uid = p.get(&format!("CreateVolumePermission.{kind}.{n}.UserId"));
            let grp = p.get(&format!("CreateVolumePermission.{kind}.{n}.Group"));
            match (uid, grp) {
                (Some(u), _) => out.push(u.clone()),
                (None, Some(g)) => {
                    validate_group(g)?;
                    out.push("all".to_string());
                }
                (None, None) => break,
            }
            n += 1;
        }
        Ok(out)
    };
    let mut add = collect("Add")?;
    let mut remove = collect("Remove")?;
    if add.is_empty() && remove.is_empty() {
        let op = p.get("OperationType").map(|s| s.as_str()).unwrap_or("");
        let mut legacy = indexed_list(p, "UserId");
        for g in indexed_list(p, "UserGroup") {
            validate_group(&g)?;
            legacy.push("all".to_string());
        }
        match op {
            "add" => add = legacy,
            "remove" => remove = legacy,
            _ => {}
        }
    }
    // AWS rejects a request that both adds and removes permissions in one call.
    if !add.is_empty() && !remove.is_empty() {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            "A single ModifySnapshotAttribute request cannot both add and remove createVolumePermission",
        ));
    }
    {
        let mut accounts = svc.state.write();
        let s = accounts
            .regional_mut(&req.account_id, &req.region)
            .snapshots
            .get_mut(&id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "InvalidSnapshot.NotFound",
                    format!("The snapshot '{id}' does not exist"),
                )
            })?;
        for g in add {
            if !s.create_volume_permissions.contains(&g) {
                s.create_volume_permissions.push(g);
            }
        }
        s.create_volume_permissions.retain(|g| !remove.contains(g));
    }
    Ok(Ec2Service::respond(
        "ModifySnapshotAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_snapshot_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SnapshotId")?;
    let attr = require(&req.query_params, "Attribute")?;
    validate_enum(
        &req.query_params,
        "Attribute",
        &["productCodes", "createVolumePermission"],
    )?;
    if attr == "createVolumePermission" {
        let mut accounts = svc.state.write();
        if let Some(s) = accounts
            .regional_mut(&req.account_id, &req.region)
            .snapshots
            .get_mut(&id)
        {
            s.create_volume_permissions.clear();
        }
    }
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
            .regional_mut(&req.account_id, &req.region)
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
            .regional_mut(&req.account_id, &req.region)
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
            .regional(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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

#[cfg(test)]
mod modify_tests {
    use super::*;

    fn req(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn seed_snapshot(svc: &Ec2Service) {
        let mut accounts = svc.state.write();
        accounts
            .regional_mut("000000000000", "us-east-1")
            .snapshots
            .insert(
                "snap-1".to_string(),
                build_snapshot("vol-1".into(), "d".into()),
            );
    }

    fn describe_perms(svc: &Ec2Service) -> String {
        let resp = describe_snapshot_attribute(
            svc,
            &req(
                "DescribeSnapshotAttribute",
                &[
                    ("SnapshotId", "snap-1"),
                    ("Attribute", "createVolumePermission"),
                ],
            ),
        )
        .unwrap();
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn modify_snapshot_attribute_round_trips_create_volume_permission() {
        // The handler validated then dropped the grant (bug-hunt 2026-07-01).
        let svc = Ec2Service::new();
        seed_snapshot(&svc);
        modify_snapshot_attribute(
            &svc,
            &req(
                "ModifySnapshotAttribute",
                &[
                    ("SnapshotId", "snap-1"),
                    ("Attribute", "createVolumePermission"),
                    ("OperationType", "add"),
                    ("CreateVolumePermission.Add.1.UserId", "111122223333"),
                ],
            ),
        )
        .unwrap();
        assert!(describe_perms(&svc).contains("111122223333"));

        modify_snapshot_attribute(
            &svc,
            &req(
                "ModifySnapshotAttribute",
                &[
                    ("SnapshotId", "snap-1"),
                    ("Attribute", "createVolumePermission"),
                    ("OperationType", "remove"),
                    ("CreateVolumePermission.Remove.1.UserId", "111122223333"),
                ],
            ),
        )
        .unwrap();
        assert!(!describe_perms(&svc).contains("111122223333"));
    }

    #[test]
    fn reset_snapshot_attribute_clears_permissions() {
        let svc = Ec2Service::new();
        seed_snapshot(&svc);
        svc.state
            .write()
            .regional_mut("000000000000", "us-east-1")
            .snapshots
            .get_mut("snap-1")
            .unwrap()
            .create_volume_permissions = vec!["all".into()];
        reset_snapshot_attribute(
            &svc,
            &req(
                "ResetSnapshotAttribute",
                &[
                    ("SnapshotId", "snap-1"),
                    ("Attribute", "createVolumePermission"),
                ],
            ),
        )
        .unwrap();
        assert!(svc
            .state
            .read()
            .regional("000000000000", "us-east-1")
            .unwrap()
            .snapshots["snap-1"]
            .create_volume_permissions
            .is_empty());
    }

    fn expect_err(svc: &Ec2Service, query: &[(&str, &str)]) -> AwsServiceError {
        match modify_snapshot_attribute(svc, &req("ModifySnapshotAttribute", query)) {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn modify_snapshot_attribute_unknown_id_errors() {
        // Cubic P2 on #2057: unknown SnapshotId was a silent no-op success.
        let svc = Ec2Service::new();
        let err = expect_err(
            &svc,
            &[
                ("SnapshotId", "snap-missing"),
                ("Attribute", "createVolumePermission"),
                ("OperationType", "add"),
                ("CreateVolumePermission.Add.1.UserId", "111122223333"),
            ],
        );
        assert!(format!("{err:?}").contains("InvalidSnapshot.NotFound"));
    }

    #[test]
    fn modify_snapshot_attribute_rejects_add_and_remove_together() {
        let svc = Ec2Service::new();
        seed_snapshot(&svc);
        let err = expect_err(
            &svc,
            &[
                ("SnapshotId", "snap-1"),
                ("Attribute", "createVolumePermission"),
                ("CreateVolumePermission.Add.1.UserId", "111122223333"),
                ("CreateVolumePermission.Remove.1.UserId", "444455556666"),
            ],
        );
        assert!(format!("{err:?}").contains("InvalidParameterCombination"));
    }

    #[test]
    fn create_snapshot_rejects_nonexistent_volume() {
        let svc = Ec2Service::new();
        let err = match create_snapshot(&svc, &req("CreateSnapshot", &[("VolumeId", "vol-nope")])) {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.code(), "InvalidVolume.NotFound");
    }

    #[test]
    fn create_snapshot_uses_real_volume_size() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            let mut v = crate::state::Volume {
                volume_id: "vol-1".into(),
                size: 42,
                snapshot_id: None,
                availability_zone: "us-east-1a".into(),
                state: "available".into(),
                volume_type: "gp3".into(),
                iops: None,
                throughput: None,
                encrypted: false,
                kms_key_id: None,
                multi_attach_enabled: false,
                auto_enable_io: false,
                attachments: vec![],
                in_recycle_bin: false,
                modification: None,
            };
            v.size = 42;
            state.volumes.insert("vol-1".into(), v);
        }
        let resp = create_snapshot(&svc, &req("CreateSnapshot", &[("VolumeId", "vol-1")])).unwrap();
        let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        assert!(body.contains("<volumeSize>42</volumeSize>"), "{body}");
    }

    #[test]
    fn create_snapshots_builds_one_per_attached_volume() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            state.instances.insert(
                "i-1".into(),
                crate::state::Instance {
                    instance_id: "i-1".into(),
                    image_id: "ami-1".into(),
                    instance_type: "t3.micro".into(),
                    state_code: 16,
                    state_name: "running".into(),
                    private_ip: "10.0.0.5".into(),
                    public_ip: None,
                    subnet_id: None,
                    vpc_id: None,
                    key_name: None,
                    security_group_ids: vec![],
                    reservation_id: "r-1".into(),
                    ami_launch_index: 0,
                    monitoring: false,
                    az: "us-east-1a".into(),
                    launch_time: "t".into(),
                    container_id: None,
                    disable_api_termination: false,
                    disable_api_stop: false,
                    source_dest_check: true,
                    ebs_optimized: false,
                    instance_initiated_shutdown_behavior: "stop".into(),
                    user_data: None,
                    metadata_options: Default::default(),
                    cpu_options: None,
                    bandwidth_weighting: None,
                    maintenance_options: Default::default(),
                    placement_tenancy: None,
                    placement_affinity: None,
                    placement_group_name: None,
                    private_dns_hostname_type: None,
                    enable_resource_name_dns_a_record: false,
                    enable_resource_name_dns_aaaa_record: false,
                    instance_lifecycle: None,
                },
            );
            for vid in ["vol-a", "vol-b"] {
                state.volumes.insert(
                    vid.into(),
                    crate::state::Volume {
                        volume_id: vid.into(),
                        size: 10,
                        snapshot_id: None,
                        availability_zone: "us-east-1a".into(),
                        state: "in-use".into(),
                        volume_type: "gp3".into(),
                        iops: None,
                        throughput: None,
                        encrypted: false,
                        kms_key_id: None,
                        multi_attach_enabled: false,
                        auto_enable_io: false,
                        attachments: vec![crate::state::VolumeAttachment {
                            volume_id: vid.into(),
                            instance_id: "i-1".into(),
                            device: "/dev/sdf".into(),
                            status: "attached".into(),
                            delete_on_termination: false,
                        }],
                        in_recycle_bin: false,
                        modification: None,
                    },
                );
            }
        }
        let resp = create_snapshots(
            &svc,
            &req(
                "CreateSnapshots",
                &[("InstanceSpecification.InstanceId", "i-1")],
            ),
        )
        .unwrap();
        let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        assert!(body.contains("<volumeId>vol-a</volumeId>"), "{body}");
        assert!(body.contains("<volumeId>vol-b</volumeId>"), "{body}");
        // Two snapshots persisted.
        assert_eq!(
            svc.state
                .read()
                .regional("000000000000", "us-east-1")
                .unwrap()
                .snapshots
                .len(),
            2
        );
    }

    #[test]
    fn modify_snapshot_attribute_rejects_invalid_group() {
        let svc = Ec2Service::new();
        seed_snapshot(&svc);
        let err = expect_err(
            &svc,
            &[
                ("SnapshotId", "snap-1"),
                ("Attribute", "createVolumePermission"),
                ("OperationType", "add"),
                ("CreateVolumePermission.Add.1.Group", "everyone"),
            ],
        );
        assert!(format!("{err:?}").contains("InvalidParameterValue"));
    }

    fn describe_body(svc: &Ec2Service, query: &[(&str, &str)]) -> String {
        let resp = describe_snapshots(svc, &req("DescribeSnapshots", query)).unwrap();
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn describe_snapshots_unknown_id_errors() {
        let svc = Ec2Service::new();
        seed_snapshot(&svc);
        let err = crate::test_support::err_of(describe_snapshots(
            &svc,
            &req("DescribeSnapshots", &[("SnapshotId.1", "snap-missing")]),
        ));
        assert_eq!(err.code(), "InvalidSnapshot.NotFound");
    }

    #[test]
    fn describe_snapshots_rejects_below_min_max_results() {
        let svc = Ec2Service::new();
        let err = crate::test_support::err_of(describe_snapshots(
            &svc,
            &req("DescribeSnapshots", &[("MaxResults", "3")]),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn describe_snapshots_paginates() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            for i in 0..8 {
                let mut s = build_snapshot("vol-x".into(), format!("d{i}"));
                s.snapshot_id = format!("snap-{i:02}");
                state.snapshots.insert(s.snapshot_id.clone(), s);
            }
        }
        let body = describe_body(&svc, &[("MaxResults", "5")]);
        assert!(body.contains("<nextToken>"), "expected a NextToken: {body}");
        // The token round-trips to the next page.
        let token = body
            .split("<nextToken>")
            .nth(1)
            .and_then(|s| s.split("</nextToken>").next())
            .unwrap()
            .to_string();
        let page2 = describe_body(&svc, &[("MaxResults", "5"), ("NextToken", &token)]);
        assert!(
            page2.contains("<snapshotId>snap-07</snapshotId>"),
            "{page2}"
        );
    }

    #[test]
    fn describe_snapshots_owner_and_field_filters() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            let mut s = build_snapshot("vol-9".into(), "backup-nightly".into());
            s.snapshot_id = "snap-9".into();
            s.encrypted = true;
            s.volume_size = 20;
            state.snapshots.insert("snap-9".into(), s);
        }
        // owner-id matches the requesting account.
        assert!(describe_body(
            &svc,
            &[
                ("Filter.1.Name", "owner-id"),
                ("Filter.1.Value.1", "000000000000")
            ],
        )
        .contains("<snapshotId>snap-9</snapshotId>"));
        // volume-size, encrypted and description filters resolve.
        assert!(describe_body(
            &svc,
            &[("Filter.1.Name", "volume-size"), ("Filter.1.Value.1", "20")],
        )
        .contains("<snapshotId>snap-9</snapshotId>"));
        assert!(describe_body(
            &svc,
            &[("Filter.1.Name", "encrypted"), ("Filter.1.Value.1", "true")],
        )
        .contains("<snapshotId>snap-9</snapshotId>"));
        assert!(describe_body(
            &svc,
            &[
                ("Filter.1.Name", "description"),
                ("Filter.1.Value.1", "backup-nightly")
            ],
        )
        .contains("<snapshotId>snap-9</snapshotId>"));
        // A non-matching owner-id filters it out.
        assert!(!describe_body(
            &svc,
            &[
                ("Filter.1.Name", "owner-id"),
                ("Filter.1.Value.1", "999999999999")
            ],
        )
        .contains("<snapshotId>snap-9</snapshotId>"));
    }
}

#[cfg(test)]
mod tag_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, seed_instance};

    fn body_of(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn snap_ids(xml: &str) -> Vec<String> {
        xml.split("<snapshotId>")
            .skip(1)
            .filter_map(|s| s.split("</snapshotId>").next())
            .map(str::to_string)
            .collect()
    }

    fn describe(svc: &Ec2Service, id: &str) -> String {
        body_of(
            describe_snapshots(svc, &req("DescribeSnapshots", &[("SnapshotId.1", id)])).unwrap(),
        )
    }

    /// Create a volume tagged `team=core` and attach it to `instance`.
    fn tagged_attached_volume(svc: &Ec2Service, instance: &str) -> String {
        let vol = body_of(
            crate::service::volume::create_volume(
                svc,
                &req(
                    "CreateVolume",
                    &[
                        ("AvailabilityZone", "us-east-1a"),
                        ("Size", "10"),
                        ("TagSpecification.1.ResourceType", "volume"),
                        ("TagSpecification.1.Tag.1.Key", "team"),
                        ("TagSpecification.1.Tag.1.Value", "core"),
                    ],
                ),
            )
            .unwrap(),
        );
        let vol_id = vol
            .split("<volumeId>")
            .nth(1)
            .and_then(|s| s.split("</volumeId>").next())
            .unwrap()
            .to_string();
        let mut accounts = svc.state.write();
        let st = accounts.regional_mut("000000000000", "us-east-1");
        st.volumes
            .get_mut(&vol_id)
            .unwrap()
            .attachments
            .push(crate::state::VolumeAttachment {
                volume_id: vol_id.clone(),
                instance_id: instance.to_string(),
                device: "/dev/sdf".into(),
                status: "attached".into(),
                delete_on_termination: false,
            });
        vol_id
    }

    #[test]
    fn create_snapshots_copies_volume_tags_and_applies_tag_specifications() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-tags");
        tagged_attached_volume(&svc, "i-tags");
        let out = body_of(
            create_snapshots(
                &svc,
                &req(
                    "CreateSnapshots",
                    &[
                        ("InstanceSpecification.InstanceId", "i-tags"),
                        ("CopyTagsFromSource", "volume"),
                        ("TagSpecification.1.ResourceType", "snapshot"),
                        ("TagSpecification.1.Tag.1.Key", "env"),
                        ("TagSpecification.1.Tag.1.Value", "dev"),
                    ],
                ),
            )
            .unwrap(),
        );
        let ids = snap_ids(&out);
        assert_eq!(ids.len(), 1, "{out}");
        assert!(
            out.contains("<key>team</key>") && out.contains("<key>env</key>"),
            "{out}"
        );
        let d = describe(&svc, &ids[0]);
        assert!(
            d.contains("<key>team</key>") && d.contains("<value>core</value>"),
            "{d}"
        );
        assert!(
            d.contains("<key>env</key>") && d.contains("<value>dev</value>"),
            "{d}"
        );
    }

    #[test]
    fn create_snapshots_without_copy_flag_does_not_inherit_volume_tags() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-notags");
        tagged_attached_volume(&svc, "i-notags");
        let out = body_of(
            create_snapshots(
                &svc,
                &req(
                    "CreateSnapshots",
                    &[("InstanceSpecification.InstanceId", "i-notags")],
                ),
            )
            .unwrap(),
        );
        let ids = snap_ids(&out);
        assert!(!describe(&svc, &ids[0]).contains("<key>team</key>"));
    }

    #[test]
    fn copy_snapshot_applies_tag_specifications() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            accounts
                .regional_mut("000000000000", "us-east-1")
                .snapshots
                .insert(
                    "snap-src".to_string(),
                    build_snapshot("vol-1".into(), "d".into()),
                );
        }
        let out = body_of(
            copy_snapshot(
                &svc,
                &req(
                    "CopySnapshot",
                    &[
                        ("SourceRegion", "us-east-1"),
                        ("SourceSnapshotId", "snap-src"),
                        ("TagSpecification.1.ResourceType", "snapshot"),
                        ("TagSpecification.1.Tag.1.Key", "env"),
                        ("TagSpecification.1.Tag.1.Value", "copy"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains("<key>env</key>"), "{out}");
        let id = snap_ids(&out).remove(0);
        let d = describe(&svc, &id);
        assert!(
            d.contains("<key>env</key>") && d.contains("<value>copy</value>"),
            "{d}"
        );
    }
}
