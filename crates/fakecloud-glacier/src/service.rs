//! Amazon S3 Glacier REST-JSON service handler.
//!
//! Implements the full 33-operation Glacier surface: vaults
//! (create/describe/delete/list), archives (real byte upload + delete with a
//! computed SHA-256 tree hash), multipart uploads (initiate/upload-part/
//! complete/abort/list-parts/list-uploads) that assemble their parts into a
//! stored archive, retrieval and inventory jobs that settle to `Succeeded` on
//! read so `GetJobOutput` returns the archive bytes (or a JSON inventory)
//! end-to-end, vault notifications, the vault access policy, the vault-lock
//! state machine, per-vault tags, the account data-retrieval policy, and
//! provisioned capacity.
//!
//! Glacier speaks restJson1 with account-scoped paths
//! (`/{accountId}/vaults/{vaultName}/...`). The `{accountId}` segment is
//! accepted permissively: any value — including the literal `-` that AWS
//! documents as "the caller's account" — maps to the authenticated caller's
//! account, so cross-account ids are not rejected. Custom request headers
//! (`x-amz-archive-description`, `x-amz-sha256-tree-hash`, `x-amz-part-size`,
//! `Content-Range`) and response headers (`x-amz-archive-id`, `Location`,
//! `x-amz-multipart-upload-id`, `x-amz-job-id`, `x-amz-lock-id`,
//! `x-amz-capacity-id`) are mirrored per the model's `@httpHeader` bindings.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use http::header::{HeaderName, HeaderValue};
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    vault_arn, Archive, Job, MultipartUpload, NotificationConfig, Part, ProvisionedCapacity,
    SharedGlacierState, Vault, VaultLock,
};
use crate::tree_hash;

pub const GLACIER_ACTIONS: &[&str] = &[
    "CreateVault",
    "DescribeVault",
    "DeleteVault",
    "ListVaults",
    "UploadArchive",
    "DeleteArchive",
    "InitiateMultipartUpload",
    "UploadMultipartPart",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "ListMultipartUploads",
    "ListParts",
    "InitiateJob",
    "DescribeJob",
    "ListJobs",
    "GetJobOutput",
    "SetVaultAccessPolicy",
    "GetVaultAccessPolicy",
    "DeleteVaultAccessPolicy",
    "SetVaultNotifications",
    "GetVaultNotifications",
    "DeleteVaultNotifications",
    "InitiateVaultLock",
    "CompleteVaultLock",
    "GetVaultLock",
    "AbortVaultLock",
    "AddTagsToVault",
    "RemoveTagsFromVault",
    "ListTagsForVault",
    "GetDataRetrievalPolicy",
    "SetDataRetrievalPolicy",
    "ListProvisionedCapacity",
    "PurchaseProvisionedCapacity",
];

/// The routed operation plus the path parameters it needs.
#[derive(Debug)]
enum Route {
    CreateVault(String),
    DescribeVault(String),
    DeleteVault(String),
    ListVaults,
    UploadArchive(String),
    DeleteArchive { vault: String, archive_id: String },
    InitiateMultipartUpload(String),
    ListMultipartUploads(String),
    UploadMultipartPart { vault: String, upload_id: String },
    CompleteMultipartUpload { vault: String, upload_id: String },
    ListParts { vault: String, upload_id: String },
    AbortMultipartUpload { vault: String, upload_id: String },
    InitiateJob(String),
    ListJobs(String),
    DescribeJob { vault: String, job_id: String },
    GetJobOutput { vault: String, job_id: String },
    SetVaultAccessPolicy(String),
    GetVaultAccessPolicy(String),
    DeleteVaultAccessPolicy(String),
    SetVaultNotifications(String),
    GetVaultNotifications(String),
    DeleteVaultNotifications(String),
    InitiateVaultLock(String),
    CompleteVaultLock { vault: String, lock_id: String },
    GetVaultLock(String),
    AbortVaultLock(String),
    AddTagsToVault(String),
    RemoveTagsFromVault(String),
    ListTagsForVault(String),
    GetDataRetrievalPolicy,
    SetDataRetrievalPolicy,
    ListProvisionedCapacity,
    PurchaseProvisionedCapacity,
}

impl Route {
    fn action_name(&self) -> &'static str {
        match self {
            Route::CreateVault(_) => "CreateVault",
            Route::DescribeVault(_) => "DescribeVault",
            Route::DeleteVault(_) => "DeleteVault",
            Route::ListVaults => "ListVaults",
            Route::UploadArchive(_) => "UploadArchive",
            Route::DeleteArchive { .. } => "DeleteArchive",
            Route::InitiateMultipartUpload(_) => "InitiateMultipartUpload",
            Route::ListMultipartUploads(_) => "ListMultipartUploads",
            Route::UploadMultipartPart { .. } => "UploadMultipartPart",
            Route::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            Route::ListParts { .. } => "ListParts",
            Route::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            Route::InitiateJob(_) => "InitiateJob",
            Route::ListJobs(_) => "ListJobs",
            Route::DescribeJob { .. } => "DescribeJob",
            Route::GetJobOutput { .. } => "GetJobOutput",
            Route::SetVaultAccessPolicy(_) => "SetVaultAccessPolicy",
            Route::GetVaultAccessPolicy(_) => "GetVaultAccessPolicy",
            Route::DeleteVaultAccessPolicy(_) => "DeleteVaultAccessPolicy",
            Route::SetVaultNotifications(_) => "SetVaultNotifications",
            Route::GetVaultNotifications(_) => "GetVaultNotifications",
            Route::DeleteVaultNotifications(_) => "DeleteVaultNotifications",
            Route::InitiateVaultLock(_) => "InitiateVaultLock",
            Route::CompleteVaultLock { .. } => "CompleteVaultLock",
            Route::GetVaultLock(_) => "GetVaultLock",
            Route::AbortVaultLock(_) => "AbortVaultLock",
            Route::AddTagsToVault(_) => "AddTagsToVault",
            Route::RemoveTagsFromVault(_) => "RemoveTagsFromVault",
            Route::ListTagsForVault(_) => "ListTagsForVault",
            Route::GetDataRetrievalPolicy => "GetDataRetrievalPolicy",
            Route::SetDataRetrievalPolicy => "SetDataRetrievalPolicy",
            Route::ListProvisionedCapacity => "ListProvisionedCapacity",
            Route::PurchaseProvisionedCapacity => "PurchaseProvisionedCapacity",
        }
    }

    /// Whether the routed op mutates state and therefore needs a snapshot save.
    fn mutates(&self) -> bool {
        matches!(
            self,
            Route::CreateVault(_)
                | Route::DeleteVault(_)
                | Route::UploadArchive(_)
                | Route::DeleteArchive { .. }
                | Route::InitiateMultipartUpload(_)
                | Route::UploadMultipartPart { .. }
                | Route::CompleteMultipartUpload { .. }
                | Route::AbortMultipartUpload { .. }
                // Jobs settle to Succeeded on read, which mutates persisted state.
                | Route::InitiateJob(_)
                | Route::ListJobs(_)
                | Route::DescribeJob { .. }
                | Route::GetJobOutput { .. }
                | Route::SetVaultAccessPolicy(_)
                | Route::DeleteVaultAccessPolicy(_)
                | Route::SetVaultNotifications(_)
                | Route::DeleteVaultNotifications(_)
                | Route::InitiateVaultLock(_)
                | Route::CompleteVaultLock { .. }
                | Route::AbortVaultLock(_)
                // GetVaultLock expires a stale InProgress lock on read.
                | Route::GetVaultLock(_)
                | Route::AddTagsToVault(_)
                | Route::RemoveTagsFromVault(_)
                | Route::SetDataRetrievalPolicy
                | Route::PurchaseProvisionedCapacity
        )
    }
}

pub struct GlacierService {
    state: SharedGlacierState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl GlacierService {
    pub fn new(state: SharedGlacierState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Match the request method + path onto a [`Route`]. Segments are derived
    /// from the raw path preserving internal empties, and the leading segment
    /// (the account id) is discarded — it is honoured permissively, mapping to
    /// the authenticated caller's account regardless of value.
    fn resolve_route(req: &AwsRequest) -> Option<Route> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        // Drop the account-id segment; route on everything after it.
        let rest: &[&str] = segs.get(1..).unwrap_or(&[]);
        let m = &req.method;
        match (m, rest) {
            (&Method::GET, ["vaults"]) => Some(Route::ListVaults),
            (&Method::PUT, ["vaults", v]) => Some(Route::CreateVault(decode(v))),
            (&Method::GET, ["vaults", v]) => Some(Route::DescribeVault(decode(v))),
            (&Method::DELETE, ["vaults", v]) => Some(Route::DeleteVault(decode(v))),

            (&Method::POST, ["vaults", v, "archives"]) => Some(Route::UploadArchive(decode(v))),
            (&Method::DELETE, ["vaults", v, "archives", a]) => Some(Route::DeleteArchive {
                vault: decode(v),
                archive_id: decode(a),
            }),

            (&Method::POST, ["vaults", v, "multipart-uploads"]) => {
                Some(Route::InitiateMultipartUpload(decode(v)))
            }
            (&Method::GET, ["vaults", v, "multipart-uploads"]) => {
                Some(Route::ListMultipartUploads(decode(v)))
            }
            (&Method::PUT, ["vaults", v, "multipart-uploads", u]) => {
                Some(Route::UploadMultipartPart {
                    vault: decode(v),
                    upload_id: decode(u),
                })
            }
            (&Method::POST, ["vaults", v, "multipart-uploads", u]) => {
                Some(Route::CompleteMultipartUpload {
                    vault: decode(v),
                    upload_id: decode(u),
                })
            }
            (&Method::GET, ["vaults", v, "multipart-uploads", u]) => Some(Route::ListParts {
                vault: decode(v),
                upload_id: decode(u),
            }),
            (&Method::DELETE, ["vaults", v, "multipart-uploads", u]) => {
                Some(Route::AbortMultipartUpload {
                    vault: decode(v),
                    upload_id: decode(u),
                })
            }

            (&Method::POST, ["vaults", v, "jobs"]) => Some(Route::InitiateJob(decode(v))),
            (&Method::GET, ["vaults", v, "jobs"]) => Some(Route::ListJobs(decode(v))),
            (&Method::GET, ["vaults", v, "jobs", j]) => Some(Route::DescribeJob {
                vault: decode(v),
                job_id: decode(j),
            }),
            (&Method::GET, ["vaults", v, "jobs", j, "output"]) => Some(Route::GetJobOutput {
                vault: decode(v),
                job_id: decode(j),
            }),

            (&Method::PUT, ["vaults", v, "access-policy"]) => {
                Some(Route::SetVaultAccessPolicy(decode(v)))
            }
            (&Method::GET, ["vaults", v, "access-policy"]) => {
                Some(Route::GetVaultAccessPolicy(decode(v)))
            }
            (&Method::DELETE, ["vaults", v, "access-policy"]) => {
                Some(Route::DeleteVaultAccessPolicy(decode(v)))
            }

            (&Method::PUT, ["vaults", v, "notification-configuration"]) => {
                Some(Route::SetVaultNotifications(decode(v)))
            }
            (&Method::GET, ["vaults", v, "notification-configuration"]) => {
                Some(Route::GetVaultNotifications(decode(v)))
            }
            (&Method::DELETE, ["vaults", v, "notification-configuration"]) => {
                Some(Route::DeleteVaultNotifications(decode(v)))
            }

            (&Method::POST, ["vaults", v, "lock-policy"]) => {
                Some(Route::InitiateVaultLock(decode(v)))
            }
            (&Method::GET, ["vaults", v, "lock-policy"]) => Some(Route::GetVaultLock(decode(v))),
            (&Method::DELETE, ["vaults", v, "lock-policy"]) => {
                Some(Route::AbortVaultLock(decode(v)))
            }
            (&Method::POST, ["vaults", v, "lock-policy", l]) => Some(Route::CompleteVaultLock {
                vault: decode(v),
                lock_id: decode(l),
            }),

            (&Method::GET, ["vaults", v, "tags"]) => Some(Route::ListTagsForVault(decode(v))),
            (&Method::POST, ["vaults", v, "tags"]) => {
                match req.query_params.get("operation").map(String::as_str) {
                    Some("remove") => Some(Route::RemoveTagsFromVault(decode(v))),
                    // AWS defaults the tags POST to the add operation.
                    _ => Some(Route::AddTagsToVault(decode(v))),
                }
            }

            (&Method::GET, ["policies", "data-retrieval"]) => Some(Route::GetDataRetrievalPolicy),
            (&Method::PUT, ["policies", "data-retrieval"]) => Some(Route::SetDataRetrievalPolicy),

            (&Method::GET, ["provisioned-capacity"]) => Some(Route::ListProvisionedCapacity),
            (&Method::POST, ["provisioned-capacity"]) => Some(Route::PurchaseProvisionedCapacity),

            _ => None,
        }
    }

    fn dispatch(&self, req: &AwsRequest, route: &Route) -> Result<AwsResponse, GlacierError> {
        match route {
            Route::CreateVault(v) => self.create_vault(req, v),
            Route::DescribeVault(v) => self.describe_vault(req, v),
            Route::DeleteVault(v) => self.delete_vault(req, v),
            Route::ListVaults => self.list_vaults(req),
            Route::UploadArchive(v) => self.upload_archive(req, v),
            Route::DeleteArchive { vault, archive_id } => {
                self.delete_archive(req, vault, archive_id)
            }
            Route::InitiateMultipartUpload(v) => self.initiate_multipart(req, v),
            Route::ListMultipartUploads(v) => self.list_multipart_uploads(req, v),
            Route::UploadMultipartPart { vault, upload_id } => {
                self.upload_part(req, vault, upload_id)
            }
            Route::CompleteMultipartUpload { vault, upload_id } => {
                self.complete_multipart(req, vault, upload_id)
            }
            Route::ListParts { vault, upload_id } => self.list_parts(req, vault, upload_id),
            Route::AbortMultipartUpload { vault, upload_id } => {
                self.abort_multipart(req, vault, upload_id)
            }
            Route::InitiateJob(v) => self.initiate_job(req, v),
            Route::ListJobs(v) => self.list_jobs(req, v),
            Route::DescribeJob { vault, job_id } => self.describe_job(req, vault, job_id),
            Route::GetJobOutput { vault, job_id } => self.get_job_output(req, vault, job_id),
            Route::SetVaultAccessPolicy(v) => self.set_vault_access_policy(req, v),
            Route::GetVaultAccessPolicy(v) => self.get_vault_access_policy(req, v),
            Route::DeleteVaultAccessPolicy(v) => self.delete_vault_access_policy(req, v),
            Route::SetVaultNotifications(v) => self.set_vault_notifications(req, v),
            Route::GetVaultNotifications(v) => self.get_vault_notifications(req, v),
            Route::DeleteVaultNotifications(v) => self.delete_vault_notifications(req, v),
            Route::InitiateVaultLock(v) => self.initiate_vault_lock(req, v),
            Route::CompleteVaultLock { vault, lock_id } => {
                self.complete_vault_lock(req, vault, lock_id)
            }
            Route::GetVaultLock(v) => self.get_vault_lock(req, v),
            Route::AbortVaultLock(v) => self.abort_vault_lock(req, v),
            Route::AddTagsToVault(v) => self.add_tags(req, v),
            Route::RemoveTagsFromVault(v) => self.remove_tags(req, v),
            Route::ListTagsForVault(v) => self.list_tags(req, v),
            Route::GetDataRetrievalPolicy => self.get_data_retrieval_policy(req),
            Route::SetDataRetrievalPolicy => self.set_data_retrieval_policy(req),
            Route::ListProvisionedCapacity => self.list_provisioned_capacity(req),
            Route::PurchaseProvisionedCapacity => self.purchase_provisioned_capacity(req),
        }
    }

    // ---- Vaults ----------------------------------------------------------

    fn create_vault(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, GlacierError> {
        validate_vault_name(name)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .vaults
            .entry(name.to_string())
            .or_insert_with(|| Vault {
                name: name.to_string(),
                created_at: Utc::now(),
                last_inventory_date: None,
                archives: Default::default(),
                multipart_uploads: Default::default(),
                jobs: Default::default(),
                notification_config: None,
                access_policy: None,
                lock: VaultLock::default(),
                tags: Default::default(),
            });
        let location = format!("/{}/vaults/{}", req.account_id, name);
        Ok(empty(StatusCode::CREATED).header("Location", &location))
    }

    fn describe_vault(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, GlacierError> {
        let accounts = self.state.read();
        let vault = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(name))
            .ok_or_else(|| not_found_vault(name))?;
        Ok(ok_json(vault_description(
            &req.region,
            &req.account_id,
            vault,
        )))
    }

    fn delete_vault(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.vaults.remove(name).is_none() {
            return Err(not_found_vault(name));
        }
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn list_vaults(&self, req: &AwsRequest) -> Result<AwsResponse, GlacierError> {
        let limit = parse_limit(req)?;
        let marker = req.query_params.get("marker").cloned();
        let accounts = self.state.read();
        let vaults: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.vaults
                    .values()
                    .map(|v| vault_description(&req.region, &req.account_id, v))
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = paginate_checked(&vaults, marker.as_deref(), limit)
            .map_err(|_| invalid_param("Invalid marker"))?;
        Ok(ok_json(json!({
            "VaultList": page,
            "Marker": token,
        })))
    }

    // ---- Archives --------------------------------------------------------

    fn upload_archive(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let data = req.body.to_vec();
        let computed = tree_hash::tree_hash_hex(&data);
        // Validate the client-supplied tree hash when present; accept and use
        // the computed hash otherwise so callers that omit it still round-trip.
        if let Some(client) = header(req, "x-amz-sha256-tree-hash") {
            if !client.is_empty() && !client.eq_ignore_ascii_case(&computed) {
                return Err(invalid_param(
                    "Provided 'x-amz-sha256-tree-hash' header value does not match the computed hash",
                ));
            }
        }
        let description = header(req, "x-amz-archive-description").unwrap_or_default();
        let archive_id = new_token(138);
        let size = data.len() as u64;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.archives.insert(
            archive_id.clone(),
            Archive {
                id: archive_id.clone(),
                description,
                created_at: Utc::now(),
                data,
                tree_hash: computed.clone(),
                size,
            },
        );
        let location = format!(
            "/{}/vaults/{}/archives/{}",
            req.account_id, vault, archive_id
        );
        Ok(empty(StatusCode::CREATED)
            .header("Location", &location)
            .header("x-amz-sha256-tree-hash", &computed)
            .header("x-amz-archive-id", &archive_id))
    }

    fn delete_archive(
        &self,
        req: &AwsRequest,
        vault: &str,
        archive_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.archives.remove(archive_id);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    // ---- Multipart uploads ----------------------------------------------

    fn initiate_multipart(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let part_size: u64 = match header(req, "x-amz-part-size") {
            Some(s) => s
                .parse()
                .map_err(|_| invalid_param("x-amz-part-size must be an integer"))?,
            None => return Err(missing_param("x-amz-part-size")),
        };
        if part_size == 0
            || !part_size.is_multiple_of(1024 * 1024)
            || !is_power_of_two(part_size / (1024 * 1024))
        {
            return Err(invalid_param(
                "The part size must be a megabyte (1024 KB) multiplied by a power of 2",
            ));
        }
        let description = header(req, "x-amz-archive-description").unwrap_or_default();
        let upload_id = new_token(92);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.multipart_uploads.insert(
            upload_id.clone(),
            MultipartUpload {
                id: upload_id.clone(),
                archive_description: description,
                part_size,
                created_at: Utc::now(),
                parts: Default::default(),
            },
        );
        let location = format!(
            "/{}/vaults/{}/multipart-uploads/{}",
            req.account_id, vault, upload_id
        );
        Ok(empty(StatusCode::CREATED)
            .header("Location", &location)
            .header("x-amz-multipart-upload-id", &upload_id))
    }

    fn upload_part(
        &self,
        req: &AwsRequest,
        vault: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let range = header(req, "content-range").ok_or_else(|| missing_param("Content-Range"))?;
        let (start, end) = parse_content_range(&range)
            .ok_or_else(|| invalid_param("Invalid Content-Range header"))?;
        let data = req.body.to_vec();
        let computed = tree_hash::tree_hash_hex(&data);
        if let Some(client) = header(req, "x-amz-sha256-tree-hash") {
            if !client.is_empty() && !client.eq_ignore_ascii_case(&computed) {
                return Err(invalid_param(
                    "Provided 'x-amz-sha256-tree-hash' header value does not match the computed hash",
                ));
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        let upload = v
            .multipart_uploads
            .get_mut(upload_id)
            .ok_or_else(|| not_found_upload(upload_id))?;
        upload.parts.insert(
            start,
            Part {
                range_start: start,
                range_end: end,
                data,
                tree_hash: computed.clone(),
            },
        );
        Ok(empty(StatusCode::NO_CONTENT).header("x-amz-sha256-tree-hash", &computed))
    }

    fn complete_multipart(
        &self,
        req: &AwsRequest,
        vault: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let declared_size: Option<u64> = header(req, "x-amz-archive-size")
            .map(|s| {
                s.parse()
                    .map_err(|_| invalid_param("x-amz-archive-size must be an integer"))
            })
            .transpose()?;
        let declared_hash = header(req, "x-amz-sha256-tree-hash");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        let upload = v
            .multipart_uploads
            .get(upload_id)
            .ok_or_else(|| not_found_upload(upload_id))?;

        // Assemble parts in ascending range order.
        let mut data: Vec<u8> = Vec::new();
        let mut part_hashes: Vec<[u8; 32]> = Vec::new();
        for part in upload.parts.values() {
            data.extend_from_slice(&part.data);
            part_hashes.push(
                tree_hash::hex_decode(&part.tree_hash)
                    .unwrap_or_else(|| tree_hash::tree_hash_bytes(&part.data)),
            );
        }
        let total = data.len() as u64;
        if let Some(sz) = declared_size {
            if sz != total {
                return Err(invalid_param(
                    "The archive-size does not match the total size of the uploaded parts",
                ));
            }
        }
        let combined = tree_hash::combine_tree_hashes(&part_hashes);
        let combined_hex = bytes_to_hex(&combined);
        if let Some(dh) = &declared_hash {
            if !dh.is_empty() && !dh.eq_ignore_ascii_case(&combined_hex) {
                return Err(invalid_param(
                    "The tree hash does not match the uploaded parts",
                ));
            }
        }
        let archive_id = new_token(138);
        let description = upload.archive_description.clone();
        v.multipart_uploads.remove(upload_id);
        v.archives.insert(
            archive_id.clone(),
            Archive {
                id: archive_id.clone(),
                description,
                created_at: Utc::now(),
                data,
                tree_hash: combined_hex.clone(),
                size: total,
            },
        );
        let location = format!(
            "/{}/vaults/{}/archives/{}",
            req.account_id, vault, archive_id
        );
        Ok(empty(StatusCode::CREATED)
            .header("Location", &location)
            .header("x-amz-sha256-tree-hash", &combined_hex)
            .header("x-amz-archive-id", &archive_id))
    }

    fn abort_multipart(
        &self,
        req: &AwsRequest,
        vault: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        if v.multipart_uploads.remove(upload_id).is_none() {
            return Err(not_found_upload(upload_id));
        }
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn list_multipart_uploads(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let limit = parse_limit(req)?;
        let marker = req.query_params.get("marker").cloned();
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found_vault(vault))?;
        let arn = vault_arn(&req.region, &req.account_id, vault);
        let uploads: Vec<Value> = v
            .multipart_uploads
            .values()
            .map(|u| {
                json!({
                    "MultipartUploadId": u.id,
                    "VaultARN": arn,
                    "ArchiveDescription": empty_to_null(&u.archive_description),
                    "PartSizeInBytes": u.part_size,
                    "CreationDate": fmt_date(u.created_at),
                })
            })
            .collect();
        let (page, token) = paginate_checked(&uploads, marker.as_deref(), limit)
            .map_err(|_| invalid_param("Invalid marker"))?;
        Ok(ok_json(json!({ "UploadsList": page, "Marker": token })))
    }

    fn list_parts(
        &self,
        req: &AwsRequest,
        vault: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let limit = parse_limit(req)?;
        let marker = req.query_params.get("marker").cloned();
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found_vault(vault))?;
        let upload = v
            .multipart_uploads
            .get(upload_id)
            .ok_or_else(|| not_found_upload(upload_id))?;
        let arn = vault_arn(&req.region, &req.account_id, vault);
        let parts: Vec<Value> = upload
            .parts
            .values()
            .map(|p| {
                json!({
                    "RangeInBytes": format!("{}-{}", p.range_start, p.range_end),
                    "SHA256TreeHash": p.tree_hash,
                })
            })
            .collect();
        let (page, token) = paginate_checked(&parts, marker.as_deref(), limit)
            .map_err(|_| invalid_param("Invalid marker"))?;
        Ok(ok_json(json!({
            "MultipartUploadId": upload.id,
            "VaultARN": arn,
            "ArchiveDescription": empty_to_null(&upload.archive_description),
            "PartSizeInBytes": upload.part_size,
            "CreationDate": fmt_date(upload.created_at),
            "Parts": page,
            "Marker": token,
        })))
    }

    // ---- Jobs ------------------------------------------------------------

    fn initiate_job(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let params = req.json_body();
        if !params.is_object() {
            return Err(missing_param("jobParameters"));
        }
        let job_type = params
            .get("Type")
            .and_then(Value::as_str)
            .ok_or_else(|| missing_param("Type"))?
            .to_string();
        let sns_topic = params
            .get("SNSTopic")
            .and_then(Value::as_str)
            .map(str::to_string);
        let description = params
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_string);
        let tier = params
            .get("Tier")
            .and_then(Value::as_str)
            .unwrap_or("Standard")
            .to_string();
        let retrieval_byte_range = params
            .get("RetrievalByteRange")
            .and_then(Value::as_str)
            .map(str::to_string);

        let job_id = new_token(92);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let arn = vault_arn(&req.region, &req.account_id, vault);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;

        let mut job = Job {
            id: job_id.clone(),
            action: String::new(),
            description,
            archive_id: None,
            created_at: Utc::now(),
            completed: false,
            completion_date: None,
            status_code: "InProgress".to_string(),
            status_message: None,
            sns_topic,
            tier,
            output: Vec::new(),
            output_content_type: "application/octet-stream".to_string(),
            archive_size: None,
            inventory_size: None,
            sha256_tree_hash: None,
            archive_sha256_tree_hash: None,
            retrieval_byte_range,
        };

        match job_type.as_str() {
            "archive-retrieval" => {
                let archive_id = params
                    .get("ArchiveId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| missing_param("ArchiveId"))?
                    .to_string();
                let archive = v
                    .archives
                    .get(&archive_id)
                    .ok_or_else(|| not_found_archive(&archive_id))?;
                job.action = "ArchiveRetrieval".to_string();
                job.archive_id = Some(archive_id);
                job.output = archive.data.clone();
                job.output_content_type = "application/octet-stream".to_string();
                job.archive_size = Some(archive.size);
                job.sha256_tree_hash = Some(archive.tree_hash.clone());
                job.archive_sha256_tree_hash = Some(archive.tree_hash.clone());
            }
            "inventory-retrieval" => {
                let inventory = build_inventory(&arn, v);
                let bytes = serde_json::to_vec(&inventory).unwrap_or_default();
                job.action = "InventoryRetrieval".to_string();
                job.inventory_size = Some(bytes.len() as u64);
                job.output = bytes;
                job.output_content_type = "application/json".to_string();
            }
            "select" => {
                // SELECT jobs run a query over an archive; we model the control
                // plane and return an empty result set as the output.
                let archive_id = params
                    .get("ArchiveId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| missing_param("ArchiveId"))?
                    .to_string();
                if !v.archives.contains_key(&archive_id) {
                    return Err(not_found_archive(&archive_id));
                }
                job.action = "Select".to_string();
                job.archive_id = Some(archive_id);
                job.output = Vec::new();
                job.output_content_type = "application/octet-stream".to_string();
            }
            other => {
                return Err(invalid_param(format!(
                    "Type must be one of archive-retrieval, inventory-retrieval, select (got '{other}')"
                )));
            }
        }

        v.jobs.insert(job_id.clone(), job);
        let location = format!("/{}/vaults/{}/jobs/{}", req.account_id, vault, job_id);
        Ok(empty(StatusCode::ACCEPTED)
            .header("Location", &location)
            .header("x-amz-job-id", &job_id))
    }

    fn describe_job(
        &self,
        req: &AwsRequest,
        vault: &str,
        job_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let arn = vault_arn(&req.region, &req.account_id, vault);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        let job = v
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| not_found_job(job_id))?;
        settle_job(job);
        Ok(ok_json(job_description(&arn, job)))
    }

    fn list_jobs(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let limit = parse_limit(req)?;
        let marker = req.query_params.get("marker").cloned();
        let filter_status = req.query_params.get("statuscode").cloned();
        let filter_completed = req
            .query_params
            .get("completed")
            .and_then(|s| s.parse::<bool>().ok());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let arn = vault_arn(&req.region, &req.account_id, vault);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        for job in v.jobs.values_mut() {
            settle_job(job);
        }
        let jobs: Vec<Value> = v
            .jobs
            .values()
            .filter(|j| {
                filter_status
                    .as_deref()
                    .is_none_or(|s| s.eq_ignore_ascii_case(&j.status_code))
            })
            .filter(|j| filter_completed.is_none_or(|c| c == j.completed))
            .map(|j| job_description(&arn, j))
            .collect();
        let (page, token) = paginate_checked(&jobs, marker.as_deref(), limit)
            .map_err(|_| invalid_param("Invalid marker"))?;
        Ok(ok_json(json!({ "JobList": page, "Marker": token })))
    }

    fn get_job_output(
        &self,
        req: &AwsRequest,
        vault: &str,
        job_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        let job = v
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| not_found_job(job_id))?;
        settle_job(job);
        if job.status_code != "Succeeded" {
            return Err(invalid_param(
                "The job is not currently available for download",
            ));
        }
        let full = &job.output;
        // Honour a Range header for partial retrieval.
        let (slice, content_range, status) = match header(req, "range") {
            Some(r) => match parse_bytes_range(&r, full.len()) {
                Some((s, e)) => (
                    full[s..=e.min(full.len().saturating_sub(1))].to_vec(),
                    Some(format!("bytes {}-{}/{}", s, e, full.len())),
                    StatusCode::PARTIAL_CONTENT,
                ),
                None => (full.clone(), None, StatusCode::OK),
            },
            None => (full.clone(), None, StatusCode::OK),
        };
        let checksum = job
            .sha256_tree_hash
            .clone()
            .unwrap_or_else(|| tree_hash::tree_hash_hex(&slice));
        let content_type = job.output_content_type.clone();
        let mut resp = AwsResponse::json(status, slice);
        resp.content_type = content_type.clone();
        set_header(&mut resp, "Content-Type", &content_type);
        set_header(&mut resp, "x-amz-sha256-tree-hash", &checksum);
        set_header(&mut resp, "Accept-Ranges", "bytes");
        if let Some(cr) = content_range {
            set_header(&mut resp, "Content-Range", &cr);
        }
        Ok(resp)
    }

    // ---- Vault access policy --------------------------------------------

    fn set_vault_access_policy(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let body = req.json_body();
        let policy = body
            .get("Policy")
            .and_then(Value::as_str)
            .ok_or_else(|| missing_param("policy.Policy"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.access_policy = Some(policy);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn get_vault_access_policy(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found_vault(vault))?;
        let policy = v
            .access_policy
            .clone()
            .ok_or_else(|| not_found_policy("access"))?;
        // The `policy` output member is @httpPayload: the body IS the
        // VaultAccessPolicy shape.
        Ok(ok_json(json!({ "Policy": policy })))
    }

    fn delete_vault_access_policy(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.access_policy = None;
        Ok(empty(StatusCode::NO_CONTENT))
    }

    // ---- Vault notifications --------------------------------------------

    fn set_vault_notifications(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let body = req.json_body();
        let sns_topic = body
            .get("SNSTopic")
            .and_then(Value::as_str)
            .map(str::to_string);
        let events = body
            .get("Events")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.notification_config = Some(NotificationConfig { sns_topic, events });
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn get_vault_notifications(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found_vault(vault))?;
        let cfg = v
            .notification_config
            .clone()
            .ok_or_else(|| not_found_policy("notification"))?;
        Ok(ok_json(json!({
            "SNSTopic": cfg.sns_topic,
            "Events": cfg.events,
        })))
    }

    fn delete_vault_notifications(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        v.notification_config = None;
        Ok(empty(StatusCode::NO_CONTENT))
    }

    // ---- Vault lock ------------------------------------------------------

    fn initiate_vault_lock(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let body = req.json_body();
        let policy = body
            .get("Policy")
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        expire_lock(&mut v.lock);
        if v.lock.state == "InProgress" {
            return Err(GlacierError::client(
                StatusCode::CONFLICT,
                "InvalidParameterValueException",
                "A vault lock is already in progress",
            ));
        }
        if v.lock.state == "Locked" {
            return Err(GlacierError::client(
                StatusCode::CONFLICT,
                "InvalidParameterValueException",
                "The vault is already locked",
            ));
        }
        let now = Utc::now();
        let lock_id = new_token(64);
        v.lock = VaultLock {
            state: "InProgress".to_string(),
            policy,
            lock_id: Some(lock_id.clone()),
            created_at: Some(now),
            expiration_date: Some(now + chrono::Duration::hours(24)),
        };
        Ok(empty(StatusCode::CREATED).header("x-amz-lock-id", &lock_id))
    }

    fn complete_vault_lock(
        &self,
        req: &AwsRequest,
        vault: &str,
        lock_id: &str,
    ) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        expire_lock(&mut v.lock);
        if v.lock.state != "InProgress" {
            return Err(invalid_param("No vault lock is in progress"));
        }
        if v.lock.lock_id.as_deref() != Some(lock_id) {
            return Err(invalid_param("The provided lock id does not match"));
        }
        v.lock.state = "Locked".to_string();
        v.lock.expiration_date = None;
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn get_vault_lock(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        expire_lock(&mut v.lock);
        if v.lock.state.is_empty() {
            return Err(not_found_policy("lock"));
        }
        Ok(ok_json(json!({
            "Policy": v.lock.policy,
            "State": v.lock.state,
            "ExpirationDate": v.lock.expiration_date.map(fmt_date),
            "CreationDate": v.lock.created_at.map(fmt_date),
        })))
    }

    fn abort_vault_lock(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        // Aborting is valid while the lock is in progress (or already expired);
        // a completed (Locked) vault lock cannot be aborted.
        if v.lock.state == "Locked" {
            return Err(invalid_param(
                "The vault lock is already locked and cannot be aborted",
            ));
        }
        v.lock = VaultLock::default();
        Ok(empty(StatusCode::NO_CONTENT))
    }

    // ---- Tags ------------------------------------------------------------

    fn add_tags(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let body = req.json_body();
        let tags = body.get("Tags").and_then(Value::as_object).cloned();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        if let Some(tags) = tags {
            for (k, val) in tags {
                if let Some(s) = val.as_str() {
                    v.tags.insert(k, s.to_string());
                }
            }
        }
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn remove_tags(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let body = req.json_body();
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let v = state
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found_vault(vault))?;
        for k in keys {
            v.tags.remove(&k);
        }
        Ok(empty(StatusCode::NO_CONTENT))
    }

    fn list_tags(&self, req: &AwsRequest, vault: &str) -> Result<AwsResponse, GlacierError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found_vault(vault))?;
        let tags: serde_json::Map<String, Value> = v
            .tags
            .iter()
            .map(|(k, val)| (k.clone(), Value::String(val.clone())))
            .collect();
        Ok(ok_json(json!({ "Tags": tags })))
    }

    // ---- Data-retrieval policy ------------------------------------------

    fn get_data_retrieval_policy(&self, req: &AwsRequest) -> Result<AwsResponse, GlacierError> {
        let accounts = self.state.read();
        let policy = accounts
            .get(&req.account_id)
            .and_then(|s| s.data_retrieval_policy.clone())
            .unwrap_or_else(default_data_retrieval_policy);
        Ok(ok_json(json!({ "Policy": policy })))
    }

    fn set_data_retrieval_policy(&self, req: &AwsRequest) -> Result<AwsResponse, GlacierError> {
        let body = req.json_body();
        let policy = body
            .get("Policy")
            .cloned()
            .ok_or_else(|| missing_param("Policy"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.data_retrieval_policy = Some(policy);
        Ok(empty(StatusCode::NO_CONTENT))
    }

    // ---- Provisioned capacity -------------------------------------------

    fn list_provisioned_capacity(&self, req: &AwsRequest) -> Result<AwsResponse, GlacierError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.provisioned_capacity
                    .iter()
                    .map(|c| {
                        json!({
                            "CapacityId": c.capacity_id,
                            "StartDate": fmt_date(c.start_date),
                            "ExpirationDate": fmt_date(c.expiration_date),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(ok_json(json!({ "ProvisionedCapacityList": list })))
    }

    fn purchase_provisioned_capacity(&self, req: &AwsRequest) -> Result<AwsResponse, GlacierError> {
        let now = Utc::now();
        let capacity_id = new_token(64);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.provisioned_capacity.push(ProvisionedCapacity {
            capacity_id: capacity_id.clone(),
            start_date: now,
            expiration_date: now + chrono::Duration::days(30),
        });
        Ok(empty(StatusCode::CREATED).header("x-amz-capacity-id", &capacity_id))
    }
}

#[async_trait]
impl AwsService for GlacierService {
    fn service_name(&self) -> &str {
        "glacier"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // An unresolved `{label}` in the path means a required URI-label member
        // (accountId, vaultName, archiveId, ...) was omitted by the caller.
        // The braces are percent-encoded on the wire (`%7B..%7D`), so decode
        // before checking. Vault names and Glacier ids never contain braces, so
        // this is an unambiguous signal of a missing required parameter.
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let path = decode(raw);
        if path.contains('{') || path.contains('}') {
            return Ok(GlacierError::client(
                StatusCode::BAD_REQUEST,
                "MissingParameterValueException",
                "A required URI path parameter is missing",
            )
            .into_response());
        }
        let Some(route) = Self::resolve_route(&req) else {
            // Not a routing miss for any implemented op — return an AWS-shaped
            // client error rather than a bare 404 so callers get a real code.
            return Ok(GlacierError::client(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("Unable to route request: {} {}", req.method, req.raw_path),
            )
            .into_response());
        };
        let mutates = route.mutates();
        let _action = route.action_name();
        let result = self.dispatch(&req, &route);
        if mutates && result.is_ok() {
            self.save_snapshot().await;
        }
        Ok(result.unwrap_or_else(GlacierError::into_response))
    }

    fn supported_actions(&self) -> &[&str] {
        GLACIER_ACTIONS
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A Glacier error, rendered as the service's `{"code","message","type"}`
/// JSON envelope with the operation's declared `@httpError` status.
#[derive(Debug)]
struct GlacierError {
    status: StatusCode,
    code: String,
    type_: String,
    message: String,
}

impl GlacierError {
    fn client(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            type_: "Client".to_string(),
            message: message.into(),
        }
    }

    fn into_response(self) -> AwsResponse {
        let body = json!({
            "code": self.code,
            "message": self.message,
            "type": self.type_,
        });
        let mut resp = AwsResponse::json_value(self.status, body);
        resp.content_type = "application/json".to_string();
        resp
    }
}

fn invalid_param(msg: impl Into<String>) -> GlacierError {
    GlacierError::client(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn missing_param(name: &str) -> GlacierError {
    GlacierError::client(
        StatusCode::BAD_REQUEST,
        "MissingParameterValueException",
        format!("Missing required parameter: {name}"),
    )
}

fn not_found_vault(name: &str) -> GlacierError {
    GlacierError::client(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("Vault not found for ARN: {name}"),
    )
}

fn not_found_archive(id: &str) -> GlacierError {
    GlacierError::client(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("The archive ID was not found: {id}"),
    )
}

fn not_found_upload(id: &str) -> GlacierError {
    GlacierError::client(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("The upload ID was not found: {id}"),
    )
}

fn not_found_job(id: &str) -> GlacierError {
    GlacierError::client(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("The job ID was not found: {id}"),
    )
}

fn not_found_policy(kind: &str) -> GlacierError {
    GlacierError::client(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("No {kind} policy is set for the vault"),
    )
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// A JSON 200 response.
fn ok_json(value: Value) -> AwsResponse {
    let mut resp = AwsResponse::json_value(StatusCode::OK, value);
    resp.content_type = "application/json".to_string();
    resp
}

/// An empty-body response with the given status (201/202/204).
fn empty(status: StatusCode) -> AwsResponse {
    let mut resp = AwsResponse::json(status, Vec::<u8>::new());
    resp.content_type = "application/json".to_string();
    resp
}

/// Small builder-style header setter used by the empty-body constructors.
trait WithHeader {
    fn header(self, name: &str, value: &str) -> Self;
}

impl WithHeader for AwsResponse {
    fn header(mut self, name: &str, value: &str) -> Self {
        set_header(&mut self, name, value);
        self
    }
}

fn set_header(resp: &mut AwsResponse, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(value),
    ) {
        resp.headers.insert(n, v);
    }
}

// ---------------------------------------------------------------------------
// Domain helpers
// ---------------------------------------------------------------------------

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn header(req: &AwsRequest, name: &str) -> Option<String> {
    req.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn fmt_date(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn empty_to_null(s: &str) -> Value {
    if s.is_empty() {
        Value::Null
    } else {
        Value::String(s.to_string())
    }
}

fn validate_vault_name(name: &str) -> Result<(), GlacierError> {
    if name.is_empty() || name.len() > 255 {
        return Err(invalid_param(
            "Vault name must be between 1 and 255 characters",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(invalid_param(
            "Vault name can only contain a-z, A-Z, 0-9, '_', '-', and '.'",
        ));
    }
    Ok(())
}

fn parse_limit(req: &AwsRequest) -> Result<usize, GlacierError> {
    match req.query_params.get("limit") {
        Some(raw) => {
            let n: i64 = raw
                .parse()
                .map_err(|_| invalid_param("limit must be an integer"))?;
            if n < 1 {
                return Err(invalid_param("limit must be a positive integer"));
            }
            Ok(n as usize)
        }
        None => Ok(1000),
    }
}

fn vault_description(region: &str, account_id: &str, v: &Vault) -> Value {
    json!({
        "VaultARN": vault_arn(region, account_id, &v.name),
        "VaultName": v.name,
        "CreationDate": fmt_date(v.created_at),
        "LastInventoryDate": v.last_inventory_date.map(fmt_date),
        "NumberOfArchives": v.archives.len() as u64,
        "SizeInBytes": v.total_size(),
    })
}

fn job_description(arn: &str, job: &Job) -> Value {
    json!({
        "JobId": job.id,
        "JobDescription": job.description,
        "Action": job.action,
        "ArchiveId": job.archive_id,
        "VaultARN": arn,
        "CreationDate": fmt_date(job.created_at),
        "Completed": job.completed,
        "StatusCode": job.status_code,
        "StatusMessage": job.status_message,
        "ArchiveSizeInBytes": job.archive_size,
        "InventorySizeInBytes": job.inventory_size,
        "SNSTopic": job.sns_topic,
        "CompletionDate": job.completion_date.map(fmt_date),
        "SHA256TreeHash": job.sha256_tree_hash,
        "ArchiveSHA256TreeHash": job.archive_sha256_tree_hash,
        "RetrievalByteRange": job.retrieval_byte_range,
        "Tier": job.tier,
    })
}

fn build_inventory(arn: &str, v: &Vault) -> Value {
    let archives: Vec<Value> = v
        .archives
        .values()
        .map(|a| {
            json!({
                "ArchiveId": a.id,
                "ArchiveDescription": a.description,
                "CreationDate": fmt_date(a.created_at),
                "Size": a.size,
                "SHA256TreeHash": a.tree_hash,
            })
        })
        .collect();
    json!({
        "VaultARN": arn,
        "InventoryDate": fmt_date(Utc::now()),
        "ArchiveList": archives,
    })
}

fn default_data_retrieval_policy() -> Value {
    // AWS reports a default `BytesPerHour` policy for accounts that have not
    // set one; matches the documented API example.
    json!({ "Rules": [ { "Strategy": "BytesPerHour", "BytesPerHour": 10_737_418_240i64 } ] })
}

/// Settle an `InProgress` job to `Succeeded` on read, mirroring how the
/// control plane settles pending transitions when a resource is described.
fn settle_job(job: &mut Job) {
    if job.status_code == "InProgress" {
        job.status_code = "Succeeded".to_string();
        job.completed = true;
        job.completion_date = Some(Utc::now());
        job.status_message = Some("Succeeded".to_string());
    }
}

/// Expire a stale `InProgress` vault lock whose 24h window has elapsed.
fn expire_lock(lock: &mut VaultLock) {
    if lock.state == "InProgress" {
        if let Some(exp) = lock.expiration_date {
            if Utc::now() > exp {
                *lock = VaultLock::default();
            }
        }
    }
}

fn is_power_of_two(n: u64) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

/// Parse a `Content-Range: bytes 0-4194303/*` header into `(start, end)`.
fn parse_content_range(s: &str) -> Option<(u64, u64)> {
    let s = s.trim();
    let rest = s.strip_prefix("bytes ").unwrap_or(s);
    let range = rest.split('/').next().unwrap_or(rest);
    let (a, b) = range.split_once('-')?;
    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
}

/// Parse a `Range: bytes=0-1023` header into an inclusive `(start, end)` byte
/// index pair clamped to `len`.
fn parse_bytes_range(s: &str, len: usize) -> Option<(usize, usize)> {
    let s = s.trim();
    let rest = s
        .strip_prefix("bytes=")
        .or_else(|| s.strip_prefix("bytes "))?;
    let range = rest.split(',').next().unwrap_or(rest);
    let (a, b) = range.split_once('-')?;
    let start: usize = a.trim().parse().ok()?;
    let end: usize = if b.trim().is_empty() {
        len.saturating_sub(1)
    } else {
        b.trim().parse().ok()?
    };
    if start > end {
        return None;
    }
    Some((start, end))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Generate an opaque, URL-safe token of `len` hex characters, in the shape of
/// a Glacier archive / upload / job / lock id.
fn new_token(len: usize) -> String {
    let mut s = String::with_capacity(len + 32);
    while s.len() < len {
        s.push_str(&uuid::Uuid::new_v4().simple().to_string());
    }
    s.truncate(len);
    s
}
