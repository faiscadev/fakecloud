//! Amazon EFS (`elasticfilesystem`) restJson1 dispatch + operation handlers.
//!
//! The full 31-operation EFS control plane. Requests are routed to an operation
//! by HTTP method + `@http` URI path; the single path label each route carries
//! is captured positionally and query parameters are read from the raw query
//! string so repeated multi-value keys survive. State is account-partitioned
//! and persisted; each file system / mount target / access point is stored as
//! its already-output-valid `Description` object so `Describe` echoes exactly
//! what `Create`/`Put`/`Update` persisted.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use parking_lot::RwLockWriteGuard;
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{EfsData, SharedEfsState};

/// Every operation name in the EFS Smithy model (31 operations).
pub const EFS_ACTIONS: &[&str] = &[
    "CreateAccessPoint",
    "CreateFileSystem",
    "CreateMountTarget",
    "CreateReplicationConfiguration",
    "CreateTags",
    "DeleteAccessPoint",
    "DeleteFileSystem",
    "DeleteFileSystemPolicy",
    "DeleteMountTarget",
    "DeleteReplicationConfiguration",
    "DeleteTags",
    "DescribeAccessPoints",
    "DescribeAccountPreferences",
    "DescribeBackupPolicy",
    "DescribeFileSystemPolicy",
    "DescribeFileSystems",
    "DescribeLifecycleConfiguration",
    "DescribeMountTargetSecurityGroups",
    "DescribeMountTargets",
    "DescribeReplicationConfigurations",
    "DescribeTags",
    "ListTagsForResource",
    "ModifyMountTargetSecurityGroups",
    "PutAccountPreferences",
    "PutBackupPolicy",
    "PutFileSystemPolicy",
    "PutLifecycleConfiguration",
    "TagResource",
    "UntagResource",
    "UpdateFileSystem",
    "UpdateFileSystemProtection",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "CreateAccessPoint",
    "CreateFileSystem",
    "CreateMountTarget",
    "CreateReplicationConfiguration",
    "CreateTags",
    "DeleteAccessPoint",
    "DeleteFileSystem",
    "DeleteFileSystemPolicy",
    "DeleteMountTarget",
    "DeleteReplicationConfiguration",
    "DeleteTags",
    "ModifyMountTargetSecurityGroups",
    "PutAccountPreferences",
    "PutBackupPolicy",
    "PutFileSystemPolicy",
    "PutLifecycleConfiguration",
    "TagResource",
    "UntagResource",
    "UpdateFileSystem",
    "UpdateFileSystemProtection",
];

pub struct EfsService {
    state: SharedEfsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// Optional handle to EC2 state so a mount target's Availability Zone, VPC,
    /// and IP address resolve from the real subnet it references. `None` in
    /// memory-only contexts (unit tests), where the values are synthesized
    /// deterministically from the subnet id instead.
    ec2_state: Option<fakecloud_ec2::SharedEc2State>,
}

impl EfsService {
    pub fn new(state: SharedEfsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            ec2_state: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_ec2_state(mut self, ec2_state: fakecloud_ec2::SharedEc2State) -> Self {
        self.ec2_state = Some(ec2_state);
        self
    }

    /// A whole-state persist hook the CloudFormation service invokes after a
    /// stack op that touched EFS, so a CFN-created (or CFN-deleted) file system /
    /// mount target / access point is written through to disk the same way a
    /// direct mutating API call would (#1766 phantom-resource class). `None`
    /// when no snapshot store is configured (memory mode).
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Resolve a subnet's `(availability_zone, availability_zone_id, vpc_id)`
    /// from EC2 state when the subnet is real. Returns `None` when EC2 state is
    /// not wired or the subnet does not exist, so the caller falls back to
    /// deterministic synthesis.
    fn resolve_subnet(&self, account: &str, subnet_id: &str) -> Option<(String, String, String)> {
        let ec2 = self.ec2_state.as_ref()?;
        let guard = ec2.read();
        let subnet = guard.get(account)?.subnets.get(subnet_id)?;
        Some((
            subnet.availability_zone.clone(),
            subnet.availability_zone_id.clone(),
            subnet.vpc_id.clone(),
        ))
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Route a request to an operation name + captured path label by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Option<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        // A trailing empty segment is preserved on purpose: EFS's `FileSystemId`
        // / `ResourceId` / `AccessPointId` path labels have a length minimum of
        // 0, so an empty label (`.../file-systems/`) is a well-formed request
        // that must still route to its handler (which then returns the
        // operation's declared not-found), not a routing miss.
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let put = m == Method::PUT;
        let del = m == Method::DELETE;
        const V: &str = "2015-02-01";
        let (action, label): (&'static str, Option<String>) = match s.as_slice() {
            // access points
            [v, "access-points"] if *v == V && post => ("CreateAccessPoint", None),
            [v, "access-points"] if *v == V && get => ("DescribeAccessPoints", None),
            [v, "access-points", id] if *v == V && del => {
                ("DeleteAccessPoint", Some((*id).to_string()))
            }
            // mount targets
            [v, "mount-targets"] if *v == V && post => ("CreateMountTarget", None),
            [v, "mount-targets"] if *v == V && get => ("DescribeMountTargets", None),
            [v, "mount-targets", id] if *v == V && del => {
                ("DeleteMountTarget", Some((*id).to_string()))
            }
            [v, "mount-targets", id, "security-groups"] if *v == V && get => {
                ("DescribeMountTargetSecurityGroups", Some((*id).to_string()))
            }
            [v, "mount-targets", id, "security-groups"] if *v == V && put => {
                ("ModifyMountTargetSecurityGroups", Some((*id).to_string()))
            }
            // account preferences
            [v, "account-preferences"] if *v == V && get => ("DescribeAccountPreferences", None),
            [v, "account-preferences"] if *v == V && put => ("PutAccountPreferences", None),
            // file systems: fixed sub-paths before the {FileSystemId} catch-all
            [v, "file-systems"] if *v == V && post => ("CreateFileSystem", None),
            [v, "file-systems"] if *v == V && get => ("DescribeFileSystems", None),
            [v, "file-systems", "replication-configurations"] if *v == V && get => {
                ("DescribeReplicationConfigurations", None)
            }
            [v, "file-systems", id, "replication-configuration"] if *v == V && post => {
                ("CreateReplicationConfiguration", Some((*id).to_string()))
            }
            [v, "file-systems", id, "replication-configuration"] if *v == V && del => {
                ("DeleteReplicationConfiguration", Some((*id).to_string()))
            }
            [v, "file-systems", id, "policy"] if *v == V && get => {
                ("DescribeFileSystemPolicy", Some((*id).to_string()))
            }
            [v, "file-systems", id, "policy"] if *v == V && put => {
                ("PutFileSystemPolicy", Some((*id).to_string()))
            }
            [v, "file-systems", id, "policy"] if *v == V && del => {
                ("DeleteFileSystemPolicy", Some((*id).to_string()))
            }
            [v, "file-systems", id, "backup-policy"] if *v == V && get => {
                ("DescribeBackupPolicy", Some((*id).to_string()))
            }
            [v, "file-systems", id, "backup-policy"] if *v == V && put => {
                ("PutBackupPolicy", Some((*id).to_string()))
            }
            [v, "file-systems", id, "lifecycle-configuration"] if *v == V && get => {
                ("DescribeLifecycleConfiguration", Some((*id).to_string()))
            }
            [v, "file-systems", id, "lifecycle-configuration"] if *v == V && put => {
                ("PutLifecycleConfiguration", Some((*id).to_string()))
            }
            [v, "file-systems", id, "protection"] if *v == V && put => {
                ("UpdateFileSystemProtection", Some((*id).to_string()))
            }
            [v, "file-systems", id] if *v == V && del => {
                ("DeleteFileSystem", Some((*id).to_string()))
            }
            [v, "file-systems", id] if *v == V && put => {
                ("UpdateFileSystem", Some((*id).to_string()))
            }
            // tags (deprecated per-file-system API)
            [v, "create-tags", id] if *v == V && post => ("CreateTags", Some((*id).to_string())),
            [v, "delete-tags", id] if *v == V && post => ("DeleteTags", Some((*id).to_string())),
            [v, "tags", id] if *v == V && get => ("DescribeTags", Some((*id).to_string())),
            // resource tagging API
            [v, "resource-tags", id] if *v == V && get => {
                ("ListTagsForResource", Some((*id).to_string()))
            }
            [v, "resource-tags", id] if *v == V && post => ("TagResource", Some((*id).to_string())),
            [v, "resource-tags", id] if *v == V && del => {
                ("UntagResource", Some((*id).to_string()))
            }
            _ => return None,
        };
        Some((action, label))
    }
}

#[async_trait]
impl AwsService for EfsService {
    fn service_name(&self) -> &str {
        "elasticfilesystem"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, label)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, label, &req);
        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        EFS_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl EfsService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        action: &str,
        label: Option<String>,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req)?;
        crate::validate::validate_input(action, &body)?;
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        crate::validate::validate_query(&q)?;
        let label = label.unwrap_or_default();
        match action {
            "CreateFileSystem" => self.create_file_system(&ctx, &body),
            "DescribeFileSystems" => self.describe_file_systems(&ctx, &q),
            "DeleteFileSystem" => self.delete_file_system(&ctx, &label),
            "UpdateFileSystem" => self.update_file_system(&ctx, &label, &body),
            "UpdateFileSystemProtection" => self.update_file_system_protection(&ctx, &label, &body),
            "CreateMountTarget" => self.create_mount_target(&ctx, &body),
            "DescribeMountTargets" => self.describe_mount_targets(&ctx, &q),
            "DeleteMountTarget" => self.delete_mount_target(&ctx, &label),
            "DescribeMountTargetSecurityGroups" => self.describe_mt_security_groups(&ctx, &label),
            "ModifyMountTargetSecurityGroups" => {
                self.modify_mt_security_groups(&ctx, &label, &body)
            }
            "CreateAccessPoint" => self.create_access_point(&ctx, &body),
            "DescribeAccessPoints" => self.describe_access_points(&ctx, &q),
            "DeleteAccessPoint" => self.delete_access_point(&ctx, &label),
            "PutLifecycleConfiguration" => self.put_lifecycle_configuration(&ctx, &label, &body),
            "DescribeLifecycleConfiguration" => self.describe_lifecycle_configuration(&ctx, &label),
            "PutBackupPolicy" => self.put_backup_policy(&ctx, &label, &body),
            "DescribeBackupPolicy" => self.describe_backup_policy(&ctx, &label),
            "PutFileSystemPolicy" => self.put_file_system_policy(&ctx, &label, &body),
            "DescribeFileSystemPolicy" => self.describe_file_system_policy(&ctx, &label),
            "DeleteFileSystemPolicy" => self.delete_file_system_policy(&ctx, &label),
            "CreateReplicationConfiguration" => {
                self.create_replication_configuration(&ctx, &label, &body)
            }
            "DescribeReplicationConfigurations" => {
                self.describe_replication_configurations(&ctx, &q)
            }
            "DeleteReplicationConfiguration" => self.delete_replication_configuration(&ctx, &label),
            "CreateTags" => self.create_tags(&ctx, &label, &body),
            "DeleteTags" => self.delete_tags(&ctx, &label, &body),
            "DescribeTags" => self.describe_tags(&ctx, &label, &q),
            "TagResource" => self.tag_resource(&ctx, &label, &body),
            "UntagResource" => self.untag_resource(&ctx, &label, &q),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, &label, &q),
            "DescribeAccountPreferences" => self.describe_account_preferences(&ctx),
            "PutAccountPreferences" => self.put_account_preferences(&ctx, &body),
            _ => Err(AwsServiceError::action_not_implemented(
                "elasticfilesystem",
                action,
            )),
        }
    }
}

// ===================== helpers =====================

fn ok(status: StatusCode, v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(status, v))
}

fn empty(status: StatusCode) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(status, json!({})))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(&format!("Request body is malformed: {e}")))
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequest", msg)
}

fn fs_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "FileSystemNotFound",
        format!("File system '{id}' does not exist."),
    )
}

fn mt_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "MountTargetNotFound",
        format!("Mount target '{id}' does not exist."),
    )
}

fn ap_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "AccessPointNotFound",
        format!("Access point '{id}' does not exist."),
    )
}

fn subnet_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "SubnetNotFound",
        format!("The subnet ID '{id}' is invalid or does not exist."),
    )
}

fn replication_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ReplicationNotFound",
        format!("Replication configuration for file system '{id}' does not exist."),
    )
}

fn incorrect_fs_lifecycle_state(id: &str, state: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::CONFLICT,
        "IncorrectFileSystemLifeCycleState",
        format!(
            "File system '{id}' is in life cycle state '{state}' but must be 'available' for this operation."
        ),
    )
}

fn conflict(code: &str, msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, code, msg)
}

/// 17 lowercase hex characters, matching EFS's resource-id suffix form.
fn hex17() -> String {
    Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(17)
        .collect()
}

fn now_ts() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

fn fs_arn(ctx: &Ctx, fsid: &str) -> String {
    format!(
        "arn:aws:elasticfilesystem:{}:{}:file-system/{}",
        ctx.region, ctx.account, fsid
    )
}

fn ap_arn(ctx: &Ctx, apid: &str) -> String {
    format!(
        "arn:aws:elasticfilesystem:{}:{}:access-point/{}",
        ctx.region, ctx.account, apid
    )
}

/// Normalize a `FileSystemId` label/field that may arrive as a bare `fs-...`
/// id or as a full ARN (`.../file-system/fs-...`) into the bare id.
fn normalize_fs_id(raw: &str) -> String {
    raw.rsplit('/').next().unwrap_or(raw).to_string()
}

/// FNV-1a hash for deterministic synthesis of AZ/VPC/IP from a subnet id.
fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

fn query_all(q: &[(String, String)], key: &str) -> Vec<String> {
    q.iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .collect()
}

/// Collect a resource's `Tags` input list into a flat key/value map.
fn tags_to_map(b: &Value) -> Map<String, Value> {
    let mut out = Map::new();
    if let Some(arr) = b.get("Tags").and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), json!(v));
            }
        }
    }
    out
}

/// Render a resource's stored tag map into the `[{Key,Value}]` list shape.
fn tags_list(data: &EfsData, resource_id: &str) -> Value {
    let arr: Vec<Value> = data
        .tags
        .get(resource_id)
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                .collect()
        })
        .unwrap_or_default();
    Value::Array(arr)
}

impl EfsService {
    fn account<'g>(
        &self,
        guard: &'g mut RwLockWriteGuard<'_, MultiAccountState<EfsData>>,
        ctx: &Ctx,
    ) -> &'g mut EfsData {
        guard.get_or_create(&ctx.account)
    }
}

// ===================== file systems =====================

impl EfsService {
    fn create_file_system(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let creation_token = b
            .get("CreationToken")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // CreationToken is EFS's idempotency token: a second create with the
        // same token collides.
        if let Some((existing_id, _)) = data.file_systems.iter().find(|(_, fs)| {
            fs.get("CreationToken").and_then(Value::as_str) == Some(creation_token.as_str())
        }) {
            let existing_id = existing_id.clone();
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::CONFLICT,
                "FileSystemAlreadyExists",
                format!("File system already exists with creation token {creation_token}"),
                vec![("FileSystemId".to_string(), existing_id)],
            ));
        }

        let fsid = format!("fs-{}", hex17());
        let performance_mode = b
            .get("PerformanceMode")
            .and_then(Value::as_str)
            .unwrap_or("generalPurpose");
        let throughput_mode = b
            .get("ThroughputMode")
            .and_then(Value::as_str)
            .unwrap_or("bursting");
        // ThroughputMode=provisioned requires a ProvisionedThroughputInMibps
        // (min 1 MiBps); every other mode must not carry one. Real EFS rejects
        // both cases with BadRequest.
        if throughput_mode == "provisioned" {
            match b
                .get("ProvisionedThroughputInMibps")
                .and_then(Value::as_f64)
            {
                None => {
                    return Err(bad_request(
                        "ProvisionedThroughputInMibps is required when ThroughputMode is set to provisioned.",
                    ));
                }
                Some(v) if v < 1.0 => {
                    return Err(bad_request(
                        "Value at 'ProvisionedThroughputInMibps' failed to satisfy constraint: Member must have value greater than or equal to 1",
                    ));
                }
                Some(_) => {}
            }
        } else if b.get("ProvisionedThroughputInMibps").is_some() {
            return Err(bad_request(
                "ProvisionedThroughputInMibps is only applicable when ThroughputMode is set to provisioned.",
            ));
        }
        let encrypted = b.get("Encrypted").and_then(Value::as_bool).unwrap_or(false);

        let mut fs = Map::new();
        fs.insert("OwnerId".into(), json!(ctx.account));
        fs.insert("CreationToken".into(), json!(creation_token));
        fs.insert("FileSystemId".into(), json!(fsid));
        fs.insert("FileSystemArn".into(), json!(fs_arn(ctx, &fsid)));
        fs.insert("CreationTime".into(), json!(now_ts()));
        // Return the transient state; settles to `available` on the next
        // describe (and on restart).
        fs.insert("LifeCycleState".into(), json!("creating"));
        fs.insert("NumberOfMountTargets".into(), json!(0));
        fs.insert(
            "SizeInBytes".into(),
            json!({
                "Value": 6144,
                "Timestamp": now_ts(),
                "ValueInIA": 0,
                "ValueInStandard": 6144,
                "ValueInArchive": 0
            }),
        );
        fs.insert("PerformanceMode".into(), json!(performance_mode));
        fs.insert("ThroughputMode".into(), json!(throughput_mode));
        fs.insert("Encrypted".into(), json!(encrypted));
        if encrypted {
            let kms = b
                .get("KmsKeyId")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!(
                        "arn:aws:kms:{}:{}:key/{}-{}-{}-{}-{}",
                        ctx.region,
                        ctx.account,
                        &hex17()[..8],
                        &hex17()[..4],
                        &hex17()[..4],
                        &hex17()[..4],
                        &hex17()[..12.min(hex17().len())]
                    )
                });
            fs.insert("KmsKeyId".into(), json!(kms));
        }
        if throughput_mode == "provisioned" {
            if let Some(p) = b.get("ProvisionedThroughputInMibps") {
                fs.insert("ProvisionedThroughputInMibps".into(), p.clone());
            }
        }
        if let Some(az) = b.get("AvailabilityZoneName").and_then(Value::as_str) {
            fs.insert("AvailabilityZoneName".into(), json!(az));
            fs.insert(
                "AvailabilityZoneId".into(),
                json!(format!("{}-az1", ctx.region)),
            );
        }
        // Tags: store, and surface Name from a `Name` tag.
        let tag_map = tags_to_map(b);
        if let Some(name) = tag_map.get("Name").and_then(Value::as_str) {
            fs.insert("Name".into(), json!(name));
        }
        fs.insert("Tags".into(), tags_list_from_map(&tag_map));
        fs.insert(
            "FileSystemProtection".into(),
            json!({ "ReplicationOverwriteProtection": "ENABLED" }),
        );

        if !tag_map.is_empty() {
            let entry = data.tags.entry(fsid.clone()).or_default();
            for (k, v) in &tag_map {
                if let Some(vs) = v.as_str() {
                    entry.insert(k.clone(), vs.to_string());
                }
            }
        }
        data.file_systems
            .insert(fsid.clone(), Value::Object(fs.clone()));
        ok(StatusCode::CREATED, Value::Object(fs))
    }

    fn describe_file_systems(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter_id = query_one(q, "FileSystemId").map(normalize_fs_id);
        let filter_token = query_one(q, "CreationToken");
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        data.reconcile_lifecycle();

        if let Some(id) = &filter_id {
            if !data.file_systems.contains_key(id) {
                return Err(fs_not_found(id));
            }
        }
        let rows: Vec<Value> = data
            .file_systems
            .values()
            .filter(|fs| {
                filter_id
                    .as_ref()
                    .map(|id| fs.get("FileSystemId").and_then(Value::as_str) == Some(id.as_str()))
                    .unwrap_or(true)
            })
            .filter(|fs| {
                filter_token
                    .map(|t| fs.get("CreationToken").and_then(Value::as_str) == Some(t))
                    .unwrap_or(true)
            })
            .map(|fs| with_live_mount_count(fs, data))
            .collect();

        // DescribeFileSystems: MaxItems default is 100.
        let (page, next) =
            paginate_marker(rows, query_one(q, "Marker"), query_one(q, "MaxItems"), 100);
        let mut out = Map::new();
        out.insert("FileSystems".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextMarker".into(), json!(n));
        }
        ok(StatusCode::OK, Value::Object(out))
    }

    fn require_fs<'a>(
        &self,
        data: &'a mut EfsData,
        fsid: &str,
    ) -> Result<&'a mut Value, AwsServiceError> {
        data.file_systems
            .get_mut(fsid)
            .ok_or_else(|| fs_not_found(fsid))
    }

    fn delete_file_system(&self, ctx: &Ctx, label: &str) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        // Real EFS returns FileSystemInUse while the file system still has any
        // mount target OR any access point; it can only be deleted once both
        // are gone.
        let has_mt = data
            .mount_targets
            .values()
            .any(|mt| mt.get("FileSystemId").and_then(Value::as_str) == Some(fsid.as_str()));
        let has_ap = data.access_points.values().any(|ap| {
            normalize_fs_id(ap.get("FileSystemId").and_then(Value::as_str).unwrap_or("")) == fsid
        });
        if has_mt || has_ap {
            return Err(conflict(
                "FileSystemInUse",
                &format!("File system '{fsid}' is in use and cannot be deleted."),
            ));
        }
        data.file_systems.remove(&fsid);
        data.tags.remove(&fsid);
        data.lifecycle_configs.remove(&fsid);
        data.backup_policies.remove(&fsid);
        data.file_system_policies.remove(&fsid);
        data.replications.remove(&fsid);
        empty(StatusCode::NO_CONTENT)
    }

    fn update_file_system(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        let fs = self.require_fs(data, &fsid)?;
        let obj = fs.as_object_mut().unwrap();
        if obj.get("LifeCycleState").and_then(Value::as_str) == Some("creating") {
            obj.insert("LifeCycleState".into(), json!("available"));
        }
        if let Some(tm) = b.get("ThroughputMode") {
            obj.insert("ThroughputMode".into(), tm.clone());
        }
        if let Some(p) = b.get("ProvisionedThroughputInMibps") {
            obj.insert("ProvisionedThroughputInMibps".into(), p.clone());
        }
        // `provisioned` -> non-provisioned drops the throughput figure.
        if obj.get("ThroughputMode").and_then(Value::as_str) != Some("provisioned") {
            obj.remove("ProvisionedThroughputInMibps");
        }
        let fs_snapshot = fs.clone();
        let updated = with_live_mount_count(&fs_snapshot, data);
        ok(StatusCode::ACCEPTED, updated)
    }

    fn update_file_system_protection(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        let fs = self.require_fs(data, &fsid)?;
        let protection = b
            .get("ReplicationOverwriteProtection")
            .and_then(Value::as_str)
            .unwrap_or("ENABLED");
        fs.as_object_mut().unwrap().insert(
            "FileSystemProtection".into(),
            json!({ "ReplicationOverwriteProtection": protection }),
        );
        ok(
            StatusCode::OK,
            json!({ "ReplicationOverwriteProtection": protection }),
        )
    }
}

fn tags_list_from_map(m: &Map<String, Value>) -> Value {
    Value::Array(
        m.iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect(),
    )
}

/// Clone a file-system description with `NumberOfMountTargets` recomputed from
/// the live mount-target set.
fn with_live_mount_count(fs: &Value, data: &EfsData) -> Value {
    let mut obj = fs.as_object().cloned().unwrap_or_default();
    let fsid = obj
        .get("FileSystemId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let count = data
        .mount_targets
        .values()
        .filter(|mt| mt.get("FileSystemId").and_then(Value::as_str) == Some(fsid))
        .count();
    obj.insert("NumberOfMountTargets".into(), json!(count));
    Value::Object(obj)
}

/// Opaque start-index pagination over EFS's `Marker` / `MaxItems` window.
/// `default_max` is the AWS-documented page size for the calling operation
/// (100 for `DescribeFileSystems`/`DescribeTags`, 10 for `DescribeMountTargets`)
/// used when the caller omits `MaxItems`.
fn paginate_marker(
    rows: Vec<Value>,
    marker: Option<&str>,
    max: Option<&str>,
    default_max: usize,
) -> (Vec<Value>, Option<String>) {
    let start = marker.and_then(|m| m.parse::<usize>().ok()).unwrap_or(0);
    let max = max
        .and_then(|m| m.parse::<usize>().ok())
        .map(|m| m.max(1))
        .unwrap_or(default_max);
    let end = start.saturating_add(max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

// ===================== mount targets =====================

impl EfsService {
    fn create_mount_target(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(b.get("FileSystemId").and_then(Value::as_str).unwrap_or(""));
        let subnet_id = b
            .get("SubnetId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        // Resolve the subnet's real Availability Zone / VPC from EC2 state when
        // available (so a mount target on a Terraform-created subnet reports the
        // true AZ and the one-per-AZ rule is exact); otherwise synthesize
        // deterministically from the subnet id.
        let h = hash_str(&subnet_id);
        let (az_name, az_id, vpc_id) = match self.resolve_subnet(&ctx.account, &subnet_id) {
            Some((az, azid, vpc)) => (az, azid, vpc),
            None => {
                // EC2 state wired but the subnet did not resolve -> the subnet
                // genuinely does not exist, which real EFS rejects with
                // `SubnetNotFound`. Only synthesize an AZ/VPC/IP when there is
                // no EC2 subsystem to validate the subnet against at all.
                if self.ec2_state.is_some() {
                    return Err(subnet_not_found(&subnet_id));
                }
                let az_index = (h % 3) as u8;
                (
                    format!("{}{}", ctx.region, (b'a' + az_index) as char),
                    format!("{}-az{}", ctx.region, az_index + 1),
                    format!("vpc-{:017x}", h & 0x000f_ffff_ffff_ffff),
                )
            }
        };
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        // The file system must exist and be `available`. A file system settles
        // `creating` -> `available` on its first describe (the same reconcile
        // the create waiter drives), so a normally-provisioned file system is
        // available by the time a mount target is created; a file system still
        // in any non-`available` state is rejected, exactly as real EFS does.
        let fs = data
            .file_systems
            .get(&fsid)
            .ok_or_else(|| fs_not_found(&fsid))?;
        let fs_state = fs
            .get("LifeCycleState")
            .and_then(Value::as_str)
            .unwrap_or("");
        if fs_state != "available" {
            return Err(incorrect_fs_lifecycle_state(&fsid, fs_state));
        }
        // One mount target per Availability Zone per file system.
        for mt in data.mount_targets.values() {
            if mt.get("FileSystemId").and_then(Value::as_str) == Some(fsid.as_str())
                && mt.get("AvailabilityZoneName").and_then(Value::as_str) == Some(az_name.as_str())
            {
                return Err(conflict(
                    "MountTargetConflict",
                    "A mount target already exists in this Availability Zone for the file system.",
                ));
            }
        }

        let mtid = format!("fsmt-{}", hex17());
        let ip = b
            .get("IpAddress")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("10.0.{}.{}", (h >> 8) % 256, h % 254 + 1));
        let eni_id = format!("eni-{}", hex17());
        let security_groups: Vec<String> = b
            .get("SecurityGroups")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| vec![format!("sg-{:017x}", h & 0x000f_ffff_ffff_ffff)]);

        let mut mt = Map::new();
        mt.insert("OwnerId".into(), json!(ctx.account));
        mt.insert("MountTargetId".into(), json!(mtid));
        mt.insert("FileSystemId".into(), json!(fsid));
        mt.insert("SubnetId".into(), json!(subnet_id));
        mt.insert("LifeCycleState".into(), json!("creating"));
        mt.insert("IpAddress".into(), json!(ip));
        mt.insert("NetworkInterfaceId".into(), json!(eni_id));
        mt.insert("AvailabilityZoneId".into(), json!(az_id));
        mt.insert("AvailabilityZoneName".into(), json!(az_name));
        mt.insert("VpcId".into(), json!(vpc_id));
        if let Some(ip6) = b.get("Ipv6Address") {
            mt.insert("Ipv6Address".into(), ip6.clone());
        }

        data.mount_target_security_groups
            .insert(mtid.clone(), security_groups);
        data.mount_targets
            .insert(mtid.clone(), Value::Object(mt.clone()));
        ok(StatusCode::OK, Value::Object(mt))
    }

    fn describe_mount_targets(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let by_fs = query_one(q, "FileSystemId").map(normalize_fs_id);
        let by_mt = query_one(q, "MountTargetId");
        let by_ap = query_one(q, "AccessPointId");
        // Real EFS requires EXACTLY ONE of these three filters. Zero is a
        // BadRequest, and so is supplying more than one.
        let filter_count = usize::from(by_fs.is_some())
            + usize::from(by_mt.is_some())
            + usize::from(by_ap.is_some());
        if filter_count != 1 {
            return Err(bad_request(
                "Exactly one of FileSystemId, MountTargetId, or AccessPointId must be specified.",
            ));
        }
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        data.reconcile_lifecycle();

        // Resolve an access-point filter to its file system.
        let ap_fs = if let Some(ap) = by_ap {
            match data.access_points.get(ap) {
                Some(ap_obj) => Some(normalize_fs_id(
                    ap_obj
                        .get("FileSystemId")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                )),
                None => return Err(ap_not_found(ap)),
            }
        } else {
            None
        };
        if let Some(fsid) = &by_fs {
            if !data.file_systems.contains_key(fsid) {
                return Err(fs_not_found(fsid));
            }
        }
        if let Some(mtid) = by_mt {
            if !data.mount_targets.contains_key(mtid) {
                return Err(mt_not_found(mtid));
            }
        }

        let rows: Vec<Value> = data
            .mount_targets
            .iter()
            .filter(|(mtid, mt)| {
                let fs = mt.get("FileSystemId").and_then(Value::as_str);
                by_fs.as_deref().map(|f| fs == Some(f)).unwrap_or(true)
                    && by_mt.map(|m| mtid.as_str() == m).unwrap_or(true)
                    && ap_fs.as_deref().map(|f| fs == Some(f)).unwrap_or(true)
            })
            .map(|(_, mt)| mt.clone())
            .collect();

        // DescribeMountTargets: MaxItems default is 10.
        let (page, next) =
            paginate_marker(rows, query_one(q, "Marker"), query_one(q, "MaxItems"), 10);
        let mut out = Map::new();
        out.insert("MountTargets".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextMarker".into(), json!(n));
        }
        ok(StatusCode::OK, Value::Object(out))
    }

    fn delete_mount_target(&self, ctx: &Ctx, label: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if data.mount_targets.remove(label).is_none() {
            return Err(mt_not_found(label));
        }
        data.mount_target_security_groups.remove(label);
        empty(StatusCode::NO_CONTENT)
    }

    fn describe_mt_security_groups(
        &self,
        ctx: &Ctx,
        label: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        data.reconcile_lifecycle();
        if !data.mount_targets.contains_key(label) {
            return Err(mt_not_found(label));
        }
        let sgs = data
            .mount_target_security_groups
            .get(label)
            .cloned()
            .unwrap_or_default();
        ok(StatusCode::OK, json!({ "SecurityGroups": sgs }))
    }

    fn modify_mt_security_groups(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.mount_targets.contains_key(label) {
            return Err(mt_not_found(label));
        }
        let sgs: Vec<String> = b
            .get("SecurityGroups")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        data.mount_target_security_groups
            .insert(label.to_string(), sgs);
        empty(StatusCode::NO_CONTENT)
    }
}

// ===================== access points =====================

impl EfsService {
    fn create_access_point(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let client_token = b
            .get("ClientToken")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let fsid = normalize_fs_id(b.get("FileSystemId").and_then(Value::as_str).unwrap_or(""));
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        if let Some((existing_id, _)) = data.access_points.iter().find(|(_, ap)| {
            ap.get("ClientToken").and_then(Value::as_str) == Some(client_token.as_str())
        }) {
            let existing_id = existing_id.clone();
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::CONFLICT,
                "AccessPointAlreadyExists",
                format!("Access point already exists with client token {client_token}"),
                vec![("AccessPointId".to_string(), existing_id)],
            ));
        }

        let apid = format!("fsap-{}", hex17());
        let tag_map = tags_to_map(b);
        let mut ap = Map::new();
        ap.insert("ClientToken".into(), json!(client_token));
        if let Some(name) = tag_map.get("Name").and_then(Value::as_str) {
            ap.insert("Name".into(), json!(name));
        }
        ap.insert("Tags".into(), tags_list_from_map(&tag_map));
        ap.insert("AccessPointId".into(), json!(apid));
        ap.insert("AccessPointArn".into(), json!(ap_arn(ctx, &apid)));
        ap.insert("FileSystemId".into(), json!(fsid));
        if let Some(pu) = b.get("PosixUser") {
            ap.insert("PosixUser".into(), pu.clone());
        }
        if let Some(rd) = b.get("RootDirectory") {
            ap.insert("RootDirectory".into(), rd.clone());
        } else {
            ap.insert("RootDirectory".into(), json!({ "Path": "/" }));
        }
        ap.insert("OwnerId".into(), json!(ctx.account));
        ap.insert("LifeCycleState".into(), json!("creating"));

        if !tag_map.is_empty() {
            let entry = data.tags.entry(apid.clone()).or_default();
            for (k, v) in &tag_map {
                if let Some(vs) = v.as_str() {
                    entry.insert(k.clone(), vs.to_string());
                }
            }
        }
        data.access_points
            .insert(apid.clone(), Value::Object(ap.clone()));
        ok(StatusCode::OK, Value::Object(ap))
    }

    fn describe_access_points(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let by_ap = query_one(q, "AccessPointId");
        let by_fs = query_one(q, "FileSystemId").map(normalize_fs_id);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        data.reconcile_lifecycle();

        if let Some(ap) = by_ap {
            if !data.access_points.contains_key(ap) {
                return Err(ap_not_found(ap));
            }
        }
        if let Some(fsid) = &by_fs {
            if !data.file_systems.contains_key(fsid) {
                return Err(fs_not_found(fsid));
            }
        }
        let rows: Vec<Value> = data
            .access_points
            .iter()
            .filter(|(apid, ap)| {
                by_ap.map(|a| apid.as_str() == a).unwrap_or(true)
                    && by_fs
                        .as_deref()
                        .map(|f| ap.get("FileSystemId").and_then(Value::as_str) == Some(f))
                        .unwrap_or(true)
            })
            .map(|(_, ap)| ap.clone())
            .collect();

        // AccessPoints uses NextToken pagination; MaxResults default is 100.
        let start = query_one(q, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let max = query_one(q, "MaxResults")
            .and_then(|m| m.parse::<usize>().ok())
            .map(|m| m.max(1))
            .unwrap_or(100);
        let end = start.saturating_add(max).min(rows.len());
        let page = rows.get(start..end).unwrap_or(&[]).to_vec();
        let mut out = Map::new();
        out.insert("AccessPoints".into(), Value::Array(page));
        if end < rows.len() {
            out.insert("NextToken".into(), json!(end.to_string()));
        }
        ok(StatusCode::OK, Value::Object(out))
    }

    fn delete_access_point(&self, ctx: &Ctx, label: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if data.access_points.remove(label).is_none() {
            return Err(ap_not_found(label));
        }
        data.tags.remove(label);
        empty(StatusCode::NO_CONTENT)
    }
}

// ===================== lifecycle / backup / policy =====================

impl EfsService {
    fn put_lifecycle_configuration(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        let policies = b.get("LifecyclePolicies").cloned().unwrap_or(json!([]));
        data.lifecycle_configs.insert(fsid, policies.clone());
        ok(StatusCode::OK, json!({ "LifecyclePolicies": policies }))
    }

    fn describe_lifecycle_configuration(
        &self,
        ctx: &Ctx,
        label: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let exists = data
            .map(|d| d.file_systems.contains_key(&fsid))
            .unwrap_or(false);
        if !exists {
            return Err(fs_not_found(&fsid));
        }
        let policies = data
            .and_then(|d| d.lifecycle_configs.get(&fsid).cloned())
            .unwrap_or(json!([]));
        ok(StatusCode::OK, json!({ "LifecyclePolicies": policies }))
    }

    fn put_backup_policy(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        let status = b
            .get("BackupPolicy")
            .and_then(|p| p.get("Status"))
            .and_then(Value::as_str)
            .unwrap_or("DISABLED")
            .to_string();
        data.backup_policies.insert(fsid, status.clone());
        ok(
            StatusCode::OK,
            json!({ "BackupPolicy": { "Status": status } }),
        )
    }

    fn describe_backup_policy(
        &self,
        ctx: &Ctx,
        label: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let exists = data
            .map(|d| d.file_systems.contains_key(&fsid))
            .unwrap_or(false);
        if !exists {
            return Err(fs_not_found(&fsid));
        }
        let status = data
            .and_then(|d| d.backup_policies.get(&fsid).cloned())
            .unwrap_or_else(|| "DISABLED".to_string());
        ok(
            StatusCode::OK,
            json!({ "BackupPolicy": { "Status": status } }),
        )
    }

    fn put_file_system_policy(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        let policy = b
            .get("Policy")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        data.file_system_policies
            .insert(fsid.clone(), policy.clone());
        ok(
            StatusCode::OK,
            json!({ "FileSystemId": fsid, "Policy": policy }),
        )
    }

    fn describe_file_system_policy(
        &self,
        ctx: &Ctx,
        label: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let exists = data
            .map(|d| d.file_systems.contains_key(&fsid))
            .unwrap_or(false);
        if !exists {
            return Err(fs_not_found(&fsid));
        }
        match data.and_then(|d| d.file_system_policies.get(&fsid).cloned()) {
            Some(policy) => ok(
                StatusCode::OK,
                json!({ "FileSystemId": fsid, "Policy": policy }),
            ),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "PolicyNotFound",
                format!("No policy is attached to file system '{fsid}'."),
            )),
        }
    }

    fn delete_file_system_policy(
        &self,
        ctx: &Ctx,
        label: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        data.file_system_policies.remove(&fsid);
        empty(StatusCode::OK)
    }
}

// ===================== replication =====================

impl EfsService {
    fn create_replication_configuration(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        let destinations: Vec<Value> = b
            .get("Destinations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|d| {
                let region = d
                    .get("Region")
                    .and_then(Value::as_str)
                    .unwrap_or(&ctx.region)
                    .to_string();
                let dest_fs = d
                    .get("FileSystemId")
                    .and_then(Value::as_str)
                    .map(normalize_fs_id)
                    .unwrap_or_else(|| format!("fs-{}", hex17()));
                let mut dest = Map::new();
                dest.insert("Status".into(), json!("ENABLED"));
                dest.insert("FileSystemId".into(), json!(dest_fs));
                dest.insert("Region".into(), json!(region));
                dest.insert("LastReplicatedTimestamp".into(), json!(now_ts()));
                dest.insert("OwnerId".into(), json!(ctx.account));
                if let Some(role) = d.get("RoleArn") {
                    dest.insert("RoleArn".into(), role.clone());
                }
                Value::Object(dest)
            })
            .collect();

        let desc = json!({
            "SourceFileSystemId": fsid,
            "SourceFileSystemRegion": ctx.region,
            "SourceFileSystemArn": fs_arn(ctx, &fsid),
            "OriginalSourceFileSystemArn": fs_arn(ctx, &fsid),
            "CreationTime": now_ts(),
            "Destinations": destinations,
            "SourceFileSystemOwnerId": ctx.account,
        });
        data.replications.insert(fsid, desc.clone());
        ok(StatusCode::OK, desc)
    }

    fn describe_replication_configurations(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let by_fs = query_one(q, "FileSystemId").map(normalize_fs_id);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        if let Some(fsid) = &by_fs {
            let fs_exists = data
                .map(|d| d.file_systems.contains_key(fsid))
                .unwrap_or(false);
            let has_replication = data
                .map(|d| d.replications.contains_key(fsid))
                .unwrap_or(false);
            if !fs_exists && !has_replication {
                return Err(fs_not_found(fsid));
            }
            // The file system exists but has no replication configuration: real
            // EFS returns 404 ReplicationNotFound rather than an empty list.
            if !has_replication {
                return Err(replication_not_found(fsid));
            }
        }
        let rows: Vec<Value> = data
            .map(|d| {
                d.replications
                    .iter()
                    .filter(|(src, _)| by_fs.as_deref().map(|f| src.as_str() == f).unwrap_or(true))
                    .map(|(_, r)| r.clone())
                    .collect()
            })
            .unwrap_or_default();
        ok(StatusCode::OK, json!({ "Replications": rows }))
    }

    fn delete_replication_configuration(
        &self,
        ctx: &Ctx,
        label: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        if data.replications.remove(&fsid).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ReplicationNotFound",
                format!("No replication configuration found for file system '{fsid}'."),
            ));
        }
        empty(StatusCode::NO_CONTENT)
    }
}

// ===================== tagging + account preferences =====================

impl EfsService {
    fn create_tags(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        merge_tags(data, &fsid, b);
        sync_resource_tags(data, &fsid);
        empty(StatusCode::NO_CONTENT)
    }

    fn delete_tags(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        if !data.file_systems.contains_key(&fsid) {
            return Err(fs_not_found(&fsid));
        }
        if let Some(keys) = b.get("TagKeys").and_then(Value::as_array) {
            if let Some(entry) = data.tags.get_mut(&fsid) {
                for k in keys {
                    if let Some(ks) = k.as_str() {
                        entry.remove(ks);
                    }
                }
            }
        }
        sync_resource_tags(data, &fsid);
        empty(StatusCode::NO_CONTENT)
    }

    fn describe_tags(
        &self,
        ctx: &Ctx,
        label: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let fsid = normalize_fs_id(label);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        let exists = data
            .map(|d| d.file_systems.contains_key(&fsid))
            .unwrap_or(false);
        if !exists {
            return Err(fs_not_found(&fsid));
        }
        let all = data
            .map(|d| tags_list(d, &fsid))
            .unwrap_or_else(|| json!([]));
        let rows = all.as_array().cloned().unwrap_or_default();
        // DescribeTags: MaxItems default is 100.
        let (page, next) =
            paginate_marker(rows, query_one(q, "Marker"), query_one(q, "MaxItems"), 100);
        let mut out = Map::new();
        out.insert("Tags".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextMarker".into(), json!(n));
        }
        ok(StatusCode::OK, Value::Object(out))
    }

    fn tag_resource(
        &self,
        ctx: &Ctx,
        label: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        self.require_taggable(data, &rid)?;
        merge_tags(data, &rid, b);
        sync_resource_tags(data, &rid);
        empty(StatusCode::OK)
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        label: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let rid = normalize_fs_id(label);
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        self.require_taggable(data, &rid)?;
        let keys = query_all(q, "tagKeys");
        if let Some(entry) = data.tags.get_mut(&rid) {
            for k in &keys {
                entry.remove(k);
            }
        }
        sync_resource_tags(data, &rid);
        empty(StatusCode::OK)
    }

    fn list_tags_for_resource(
        &self,
        ctx: &Ctx,
        label: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let rid = normalize_fs_id(label);
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        // Validate the resource exists (fs or access point).
        let known = data
            .map(|d| {
                (rid.starts_with("fsap-") && d.access_points.contains_key(&rid))
                    || (rid.starts_with("fs-") && d.file_systems.contains_key(&rid))
            })
            .unwrap_or(false);
        if !known {
            return Err(if rid.starts_with("fsap-") {
                ap_not_found(&rid)
            } else {
                fs_not_found(&rid)
            });
        }
        let all = data
            .map(|d| tags_list(d, &rid))
            .unwrap_or_else(|| json!([]));
        let rows = all.as_array().cloned().unwrap_or_default();
        // ListTagsForResource uses NextToken pagination; MaxResults default is 100.
        let start = query_one(q, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let max = query_one(q, "MaxResults")
            .and_then(|m| m.parse::<usize>().ok())
            .map(|m| m.max(1))
            .unwrap_or(100);
        let end = start.saturating_add(max).min(rows.len());
        let page = rows.get(start..end).unwrap_or(&[]).to_vec();
        let mut out = Map::new();
        out.insert("Tags".into(), Value::Array(page));
        if end < rows.len() {
            out.insert("NextToken".into(), json!(end.to_string()));
        }
        ok(StatusCode::OK, Value::Object(out))
    }

    fn require_taggable(&self, data: &EfsData, rid: &str) -> Result<(), AwsServiceError> {
        if rid.starts_with("fsap-") {
            if data.access_points.contains_key(rid) {
                Ok(())
            } else {
                Err(ap_not_found(rid))
            }
        } else if data.file_systems.contains_key(rid) {
            Ok(())
        } else {
            Err(fs_not_found(rid))
        }
    }

    fn describe_account_preferences(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        // Real EFS returns an empty response ({}) until PutAccountPreferences
        // has been called; only then is a ResourceIdPreference present.
        match guard
            .get(&ctx.account)
            .and_then(|d| d.resource_id_preference.clone())
        {
            Some(pref) => ok(
                StatusCode::OK,
                json!({
                    "ResourceIdPreference": {
                        "ResourceIdType": pref,
                        "Resources": ["FILE_SYSTEM", "MOUNT_TARGET"]
                    }
                }),
            ),
            None => empty(StatusCode::OK),
        }
    }

    fn put_account_preferences(
        &self,
        ctx: &Ctx,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id_type = b
            .get("ResourceIdType")
            .and_then(Value::as_str)
            .unwrap_or("LONG_ID")
            .to_string();
        let mut guard = self.state.write();
        let data = self.account(&mut guard, ctx);
        data.resource_id_preference = Some(id_type.clone());
        ok(
            StatusCode::OK,
            json!({
                "ResourceIdPreference": {
                    "ResourceIdType": id_type,
                    "Resources": ["FILE_SYSTEM", "MOUNT_TARGET"]
                }
            }),
        )
    }
}

/// Merge a `Tags` input list into the account tag map under `resource_id`.
fn merge_tags(data: &mut EfsData, resource_id: &str, b: &Value) {
    let map = tags_to_map(b);
    if map.is_empty() {
        return;
    }
    let entry = data.tags.entry(resource_id.to_string()).or_default();
    for (k, v) in &map {
        if let Some(vs) = v.as_str() {
            entry.insert(k.clone(), vs.to_string());
        }
    }
}

/// Keep a taggable resource's embedded `Name` field and `Tags` list in sync
/// with its tag map after a tag mutation, so a `Describe*` reading the embedded
/// copy always agrees with `ListTagsForResource` reading the tag map. Applies to
/// both file systems (`fs-`) and access points (`fsap-`).
fn sync_resource_tags(data: &mut EfsData, rid: &str) {
    let tags = data.tags.get(rid).cloned().unwrap_or_default();
    let obj = if rid.starts_with("fsap-") {
        data.access_points.get_mut(rid)
    } else {
        data.file_systems.get_mut(rid)
    }
    .and_then(Value::as_object_mut);
    if let Some(obj) = obj {
        match tags.get("Name") {
            Some(name) => {
                obj.insert("Name".into(), json!(name));
            }
            None => {
                obj.remove("Name");
            }
        }
        obj.insert(
            "Tags".into(),
            Value::Array(
                tags.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect(),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;

    fn ctx() -> Ctx {
        Ctx {
            account: "000000000000".to_string(),
            region: "us-east-1".to_string(),
        }
    }

    fn svc() -> EfsService {
        let state: SharedEfsState = Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )));
        EfsService::new(state)
    }

    fn empty_ec2() -> fakecloud_ec2::SharedEc2State {
        Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )))
    }

    fn seed_fs(s: &EfsService, fsid: &str, life_cycle: &str) {
        let mut g = s.state.write();
        let d = g.get_or_create("000000000000");
        d.file_systems.insert(
            fsid.to_string(),
            json!({ "FileSystemId": fsid, "LifeCycleState": life_cycle }),
        );
    }

    fn body_value(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    // Defect #1: a nonexistent subnet must be rejected with SubnetNotFound once
    // EC2 state is wired to validate against (rather than synthesizing one).
    #[test]
    fn create_mount_target_nonexistent_subnet_is_subnet_not_found() {
        let s = svc().with_ec2_state(empty_ec2());
        seed_fs(&s, "fs-1", "available");
        let err = s
            .create_mount_target(
                &ctx(),
                &json!({ "FileSystemId": "fs-1", "SubnetId": "subnet-does-not-exist" }),
            )
            .err()
            .unwrap();
        assert_eq!(err.code(), "SubnetNotFound");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // Without EC2 state wired, the deterministic-synthesis fallback still
    // applies (no VPC subsystem to validate against).
    #[test]
    fn create_mount_target_without_ec2_synthesizes_subnet() {
        let s = svc();
        seed_fs(&s, "fs-1", "available");
        let resp = s
            .create_mount_target(
                &ctx(),
                &json!({ "FileSystemId": "fs-1", "SubnetId": "subnet-synth" }),
            )
            .unwrap();
        assert!(resp.status.is_success());
    }

    // Defect #4: a file system that is not yet `available` rejects a mount
    // target with IncorrectFileSystemLifeCycleState.
    #[test]
    fn create_mount_target_on_creating_fs_is_incorrect_lifecycle() {
        let s = svc();
        seed_fs(&s, "fs-1", "creating");
        let err = s
            .create_mount_target(
                &ctx(),
                &json!({ "FileSystemId": "fs-1", "SubnetId": "subnet-1" }),
            )
            .err()
            .unwrap();
        assert_eq!(err.code(), "IncorrectFileSystemLifeCycleState");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    // Defect #2: a file system with an access point cannot be deleted.
    #[test]
    fn delete_file_system_with_access_point_is_in_use() {
        let s = svc();
        seed_fs(&s, "fs-1", "available");
        {
            let mut g = s.state.write();
            let d = g.get_or_create("000000000000");
            d.access_points.insert(
                "fsap-1".to_string(),
                json!({ "AccessPointId": "fsap-1", "FileSystemId": "fs-1" }),
            );
        }
        let err = s.delete_file_system(&ctx(), "fs-1").err().unwrap();
        assert_eq!(err.code(), "FileSystemInUse");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    // Defect #3: describing replication for a file system with none returns 404
    // ReplicationNotFound, not an empty list.
    #[test]
    fn describe_replication_missing_is_replication_not_found() {
        let s = svc();
        seed_fs(&s, "fs-1", "available");
        let err = s
            .describe_replication_configurations(
                &ctx(),
                &[("FileSystemId".to_string(), "fs-1".to_string())],
            )
            .err()
            .unwrap();
        assert_eq!(err.code(), "ReplicationNotFound");
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    // With no FileSystemId filter, an empty list is correct (not an error).
    #[test]
    fn describe_replication_no_filter_is_empty_ok() {
        let s = svc();
        let resp = s.describe_replication_configurations(&ctx(), &[]).unwrap();
        assert!(resp.status.is_success());
        assert_eq!(
            body_value(&resp)["Replications"].as_array().unwrap().len(),
            0
        );
    }

    // Defect #5: DescribeMountTargets requires exactly one filter; supplying two
    // is a BadRequest.
    #[test]
    fn describe_mount_targets_two_filters_is_bad_request() {
        let s = svc();
        let err = s
            .describe_mount_targets(
                &ctx(),
                &[
                    ("FileSystemId".to_string(), "fs-1".to_string()),
                    ("MountTargetId".to_string(), "fsmt-1".to_string()),
                ],
            )
            .err()
            .unwrap();
        assert_eq!(err.code(), "BadRequest");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // Defect #8: ThroughputMode=provisioned without ProvisionedThroughputInMibps
    // is a BadRequest; the wrong-mode variant is rejected too.
    #[test]
    fn create_file_system_provisioned_without_mibps_is_bad_request() {
        let s = svc();
        let err = s
            .create_file_system(
                &ctx(),
                &json!({ "CreationToken": "tok-1", "ThroughputMode": "provisioned" }),
            )
            .err()
            .unwrap();
        assert_eq!(err.code(), "BadRequest");

        let err2 = s
            .create_file_system(
                &ctx(),
                &json!({
                    "CreationToken": "tok-2",
                    "ThroughputMode": "bursting",
                    "ProvisionedThroughputInMibps": 128
                }),
            )
            .err()
            .unwrap();
        assert_eq!(err2.code(), "BadRequest");

        // A valid provisioned request succeeds.
        let ok = s
            .create_file_system(
                &ctx(),
                &json!({
                    "CreationToken": "tok-3",
                    "ThroughputMode": "provisioned",
                    "ProvisionedThroughputInMibps": 256
                }),
            )
            .unwrap();
        assert!(ok.status.is_success());
    }

    // Defect #9: a fresh account returns an empty DescribeAccountPreferences
    // response until PutAccountPreferences is called.
    #[test]
    fn describe_account_preferences_fresh_account_is_empty() {
        let s = svc();
        let resp = s.describe_account_preferences(&ctx()).unwrap();
        assert!(resp.status.is_success());
        let v = body_value(&resp);
        assert!(
            v.get("ResourceIdPreference").is_none(),
            "expected empty {{}}, got {v}"
        );

        // After PutAccountPreferences the preference is surfaced.
        s.put_account_preferences(&ctx(), &json!({ "ResourceIdType": "SHORT_ID" }))
            .unwrap();
        let resp2 = s.describe_account_preferences(&ctx()).unwrap();
        let v2 = body_value(&resp2);
        assert_eq!(v2["ResourceIdPreference"]["ResourceIdType"], "SHORT_ID");
    }

    // Defect #10: after TagResource on an access point, the embedded Tags field
    // and ListTagsForResource must agree.
    #[test]
    fn access_point_tags_resync_agrees() {
        let s = svc();
        seed_fs(&s, "fs-1", "available");
        let created = s
            .create_access_point(
                &ctx(),
                &json!({ "ClientToken": "ct-1", "FileSystemId": "fs-1" }),
            )
            .unwrap();
        let apid = body_value(&created)["AccessPointId"]
            .as_str()
            .unwrap()
            .to_string();

        s.tag_resource(
            &ctx(),
            &apid,
            &json!({ "Tags": [{ "Key": "Env", "Value": "prod" }] }),
        )
        .unwrap();

        // Embedded Tags on the access-point object.
        let embedded: Vec<(String, String)> = {
            let g = s.state.read();
            let ap = g
                .get("000000000000")
                .unwrap()
                .access_points
                .get(&apid)
                .unwrap()
                .clone();
            ap["Tags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| {
                    (
                        t["Key"].as_str().unwrap().to_string(),
                        t["Value"].as_str().unwrap().to_string(),
                    )
                })
                .collect()
        };

        // Tags via ListTagsForResource (the tag-map source of truth).
        let listed_resp = s.list_tags_for_resource(&ctx(), &apid, &[]).unwrap();
        let listed: Vec<(String, String)> = body_value(&listed_resp)["Tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["Key"].as_str().unwrap().to_string(),
                    t["Value"].as_str().unwrap().to_string(),
                )
            })
            .collect();

        assert_eq!(embedded, vec![("Env".to_string(), "prod".to_string())]);
        assert_eq!(
            embedded, listed,
            "embedded Tags must match ListTagsForResource"
        );

        // Removing the tag keeps both views in agreement (empty).
        s.untag_resource(&ctx(), &apid, &[("tagKeys".to_string(), "Env".to_string())])
            .unwrap();
        let g = s.state.read();
        let ap = g
            .get("000000000000")
            .unwrap()
            .access_points
            .get(&apid)
            .unwrap();
        assert_eq!(ap["Tags"].as_array().unwrap().len(), 0);
    }
}
