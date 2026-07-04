//! Amazon S3 Tables (`s3tables`) restJson1 service handler.
//!
//! Implements the S3 Tables control plane: table buckets, namespaces, and
//! tables, plus every per-bucket and per-table sub-resource configuration
//! (encryption, maintenance, metrics, policy, replication, storage class,
//! record expiration) and resource tagging. The hierarchy is
//! `table bucket -> namespace -> table`; each table carries an opaque
//! `metadataLocation` S3 URI that points at its Apache Iceberg metadata
//! document — `CreateTable` seeds it, `GetTableMetadataLocation` returns it,
//! and `UpdateTableMetadataLocation` advances it (bumping the `versionToken`),
//! mirroring how the real service tracks the Iceberg metadata pointer without
//! running an Iceberg engine.
//!
//! Every operation is a restJson1 request routed on `(method, path)`. The
//! `tableBucketARN` / `resourceArn` identifiers are `@httpLabel`-bound and
//! arrive percent-encoded as a single path segment; the query-form replication
//! and record-expiration operations carry their identifiers in the query
//! string instead. Maintenance / replication / record-expiration job statuses
//! settle synchronously to a terminal `SUCCESSFUL`/idle value (no real
//! background compaction runs).

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    bucket_arn_of_table, table_arn, table_bucket_arn, table_id_of_arn, NamespaceRecord,
    SharedS3TablesState, TableBucketRecord, TableRecord, TagMap, VersionedConfig,
};
use crate::validate;

pub const S3TABLES_ACTIONS: &[&str] = &[
    "CreateNamespace",
    "CreateTable",
    "CreateTableBucket",
    "DeleteNamespace",
    "DeleteTable",
    "DeleteTableBucket",
    "DeleteTableBucketEncryption",
    "DeleteTableBucketMetricsConfiguration",
    "DeleteTableBucketPolicy",
    "DeleteTableBucketReplication",
    "DeleteTablePolicy",
    "DeleteTableReplication",
    "GetNamespace",
    "GetTable",
    "GetTableBucket",
    "GetTableBucketEncryption",
    "GetTableBucketMaintenanceConfiguration",
    "GetTableBucketMetricsConfiguration",
    "GetTableBucketPolicy",
    "GetTableBucketReplication",
    "GetTableBucketStorageClass",
    "GetTableEncryption",
    "GetTableMaintenanceConfiguration",
    "GetTableMaintenanceJobStatus",
    "GetTableMetadataLocation",
    "GetTablePolicy",
    "GetTableRecordExpirationConfiguration",
    "GetTableRecordExpirationJobStatus",
    "GetTableReplication",
    "GetTableReplicationStatus",
    "GetTableStorageClass",
    "ListNamespaces",
    "ListTableBuckets",
    "ListTables",
    "ListTagsForResource",
    "PutTableBucketEncryption",
    "PutTableBucketMaintenanceConfiguration",
    "PutTableBucketMetricsConfiguration",
    "PutTableBucketPolicy",
    "PutTableBucketReplication",
    "PutTableBucketStorageClass",
    "PutTableMaintenanceConfiguration",
    "PutTablePolicy",
    "PutTableRecordExpirationConfiguration",
    "PutTableReplication",
    "RenameTable",
    "TagResource",
    "UntagResource",
    "UpdateTableMetadataLocation",
];

/// Operations that mutate persisted state (so a snapshot save follows success).
const MUTATING: &[&str] = &[
    "CreateNamespace",
    "CreateTable",
    "CreateTableBucket",
    "DeleteNamespace",
    "DeleteTable",
    "DeleteTableBucket",
    "DeleteTableBucketEncryption",
    "DeleteTableBucketMetricsConfiguration",
    "DeleteTableBucketPolicy",
    "DeleteTableBucketReplication",
    "DeleteTablePolicy",
    "DeleteTableReplication",
    "PutTableBucketEncryption",
    "PutTableBucketMaintenanceConfiguration",
    "PutTableBucketMetricsConfiguration",
    "PutTableBucketPolicy",
    "PutTableBucketReplication",
    "PutTableBucketStorageClass",
    "PutTableMaintenanceConfiguration",
    "PutTablePolicy",
    "PutTableRecordExpirationConfiguration",
    "PutTableReplication",
    "RenameTable",
    "TagResource",
    "UntagResource",
    "UpdateTableMetadataLocation",
];

/// The decoded path labels for a route, in URI order.
type Labels = Vec<String>;

pub struct S3TablesService {
    state: SharedS3TablesState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl S3TablesService {
    pub fn new(state: SharedS3TablesState) -> Self {
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

    /// Route a request to `(action, path-labels)` by method + path segments.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Labels)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        let m = &req.method;
        let d = |s: &str| decode(s);
        let one = |a| Some((a, Vec::new()));
        macro_rules! l {
            ($a:expr, $($x:expr),*) => { Some(($a, vec![$($x),*])) };
        }
        match (m, segs.as_slice()) {
            // ---- table buckets ----
            (&Method::PUT, ["buckets"]) => one("CreateTableBucket"),
            (&Method::GET, ["buckets"]) => one("ListTableBuckets"),
            (&Method::GET, ["buckets", arn]) => l!("GetTableBucket", d(arn)),
            (&Method::DELETE, ["buckets", arn]) => l!("DeleteTableBucket", d(arn)),
            (&Method::GET, ["buckets", arn, "encryption"]) => {
                l!("GetTableBucketEncryption", d(arn))
            }
            (&Method::PUT, ["buckets", arn, "encryption"]) => {
                l!("PutTableBucketEncryption", d(arn))
            }
            (&Method::DELETE, ["buckets", arn, "encryption"]) => {
                l!("DeleteTableBucketEncryption", d(arn))
            }
            (&Method::GET, ["buckets", arn, "maintenance"]) => {
                l!("GetTableBucketMaintenanceConfiguration", d(arn))
            }
            (&Method::PUT, ["buckets", arn, "maintenance", ty]) => {
                l!("PutTableBucketMaintenanceConfiguration", d(arn), d(ty))
            }
            (&Method::GET, ["buckets", arn, "metrics"]) => {
                l!("GetTableBucketMetricsConfiguration", d(arn))
            }
            (&Method::PUT, ["buckets", arn, "metrics"]) => {
                l!("PutTableBucketMetricsConfiguration", d(arn))
            }
            (&Method::DELETE, ["buckets", arn, "metrics"]) => {
                l!("DeleteTableBucketMetricsConfiguration", d(arn))
            }
            (&Method::GET, ["buckets", arn, "policy"]) => l!("GetTableBucketPolicy", d(arn)),
            (&Method::PUT, ["buckets", arn, "policy"]) => l!("PutTableBucketPolicy", d(arn)),
            (&Method::DELETE, ["buckets", arn, "policy"]) => {
                l!("DeleteTableBucketPolicy", d(arn))
            }
            (&Method::GET, ["buckets", arn, "storage-class"]) => {
                l!("GetTableBucketStorageClass", d(arn))
            }
            (&Method::PUT, ["buckets", arn, "storage-class"]) => {
                l!("PutTableBucketStorageClass", d(arn))
            }
            // ---- namespaces ----
            (&Method::PUT, ["namespaces", arn]) => l!("CreateNamespace", d(arn)),
            (&Method::GET, ["namespaces", arn]) => l!("ListNamespaces", d(arn)),
            (&Method::GET, ["namespaces", arn, ns]) => l!("GetNamespace", d(arn), d(ns)),
            (&Method::DELETE, ["namespaces", arn, ns]) => l!("DeleteNamespace", d(arn), d(ns)),
            // ---- tables ----
            (&Method::GET, ["tables", arn]) => l!("ListTables", d(arn)),
            // GetTable's canonical model URI is `GET /get-table` (query form),
            // but the AWS SDK / terraform provider read a table via the path
            // form `GET /tables/{tableBucketARN}/{namespace}/{name}`. Support
            // both wire forms; dispatch resolves by labels when present.
            (&Method::GET, ["tables", arn, ns, name]) => {
                l!("GetTable", d(arn), d(ns), d(name))
            }
            (&Method::PUT, ["tables", arn, ns]) => l!("CreateTable", d(arn), d(ns)),
            (&Method::DELETE, ["tables", arn, ns, name]) => {
                l!("DeleteTable", d(arn), d(ns), d(name))
            }
            (&Method::PUT, ["tables", arn, ns, name, "rename"]) => {
                l!("RenameTable", d(arn), d(ns), d(name))
            }
            (&Method::GET, ["tables", arn, ns, name, "encryption"]) => {
                l!("GetTableEncryption", d(arn), d(ns), d(name))
            }
            (&Method::GET, ["tables", arn, ns, name, "maintenance"]) => {
                l!("GetTableMaintenanceConfiguration", d(arn), d(ns), d(name))
            }
            (&Method::PUT, ["tables", arn, ns, name, "maintenance", ty]) => {
                l!(
                    "PutTableMaintenanceConfiguration",
                    d(arn),
                    d(ns),
                    d(name),
                    d(ty)
                )
            }
            (&Method::GET, ["tables", arn, ns, name, "maintenance-job-status"]) => {
                l!("GetTableMaintenanceJobStatus", d(arn), d(ns), d(name))
            }
            (&Method::GET, ["tables", arn, ns, name, "metadata-location"]) => {
                l!("GetTableMetadataLocation", d(arn), d(ns), d(name))
            }
            (&Method::PUT, ["tables", arn, ns, name, "metadata-location"]) => {
                l!("UpdateTableMetadataLocation", d(arn), d(ns), d(name))
            }
            (&Method::GET, ["tables", arn, ns, name, "policy"]) => {
                l!("GetTablePolicy", d(arn), d(ns), d(name))
            }
            (&Method::PUT, ["tables", arn, ns, name, "policy"]) => {
                l!("PutTablePolicy", d(arn), d(ns), d(name))
            }
            (&Method::DELETE, ["tables", arn, ns, name, "policy"]) => {
                l!("DeleteTablePolicy", d(arn), d(ns), d(name))
            }
            (&Method::GET, ["tables", arn, ns, name, "storage-class"]) => {
                l!("GetTableStorageClass", d(arn), d(ns), d(name))
            }
            // ---- get-table (query form) ----
            (&Method::GET, ["get-table"]) => one("GetTable"),
            // ---- replication (query form) ----
            (&Method::GET, ["table-bucket-replication"]) => one("GetTableBucketReplication"),
            (&Method::PUT, ["table-bucket-replication"]) => one("PutTableBucketReplication"),
            (&Method::DELETE, ["table-bucket-replication"]) => one("DeleteTableBucketReplication"),
            (&Method::GET, ["table-replication"]) => one("GetTableReplication"),
            (&Method::PUT, ["table-replication"]) => one("PutTableReplication"),
            (&Method::DELETE, ["table-replication"]) => one("DeleteTableReplication"),
            (&Method::GET, ["replication-status"]) => one("GetTableReplicationStatus"),
            // ---- record expiration (query form) ----
            (&Method::GET, ["table-record-expiration"]) => {
                one("GetTableRecordExpirationConfiguration")
            }
            (&Method::PUT, ["table-record-expiration"]) => {
                one("PutTableRecordExpirationConfiguration")
            }
            (&Method::GET, ["table-record-expiration-job-status"]) => {
                one("GetTableRecordExpirationJobStatus")
            }
            // ---- tagging ----
            (&Method::GET, ["tag", arn]) => l!("ListTagsForResource", d(arn)),
            (&Method::POST, ["tag", arn]) => l!("TagResource", d(arn)),
            (&Method::DELETE, ["tag", arn]) => l!("UntagResource", d(arn)),
            _ => None,
        }
    }
}

#[async_trait]
impl AwsService for S3TablesService {
    fn service_name(&self) -> &str {
        "s3tables"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, labels) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let result = self.dispatch(action, &labels, &req);

        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        S3TABLES_ACTIONS
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

/// An empty-body response with an explicit status (for `Unit`-output ops).
fn empty(code: u16) -> AwsResponse {
    AwsResponse::json(
        StatusCode::from_u16(code).unwrap_or(StatusCode::OK),
        Vec::new(),
    )
}

/// RFC3339 (`date-time`) timestamp, the wire form these outputs declare.
fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn gen_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A fresh opaque concurrency token.
fn gen_token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn err(status: u16, code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
        code,
        msg,
    )
}
fn not_found(msg: impl Into<String>) -> AwsServiceError {
    err(404, "NotFoundException", msg)
}
fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    err(400, "BadRequestException", msg)
}
fn conflict(msg: impl Into<String>) -> AwsServiceError {
    err(409, "ConflictException", msg)
}

fn req_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn query(req: &AwsRequest, key: &str) -> Option<String> {
    req.query_params.get(key).cloned()
}

/// Parse the `Tags` map (`{k: v}`) from a JSON object member.
fn parse_tags(body: &Value, key: &str) -> TagMap {
    let mut out = TagMap::new();
    if let Some(obj) = body.get(key).and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn tags_value(t: &TagMap) -> Value {
    Value::Object(
        t.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

/// The default maintenance configuration AWS returns for a freshly created
/// table bucket: unreferenced-file removal enabled with 3/10 day retention.
/// Keyed by the `TableBucketMaintenanceType` enum *value*
/// (`icebergUnreferencedFileRemoval`), which is how the wire map — and the
/// terraform provider — flattens it.
fn default_bucket_maintenance() -> std::collections::BTreeMap<String, Value> {
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        "icebergUnreferencedFileRemoval".to_string(),
        json!({
            "status": "enabled",
            "settings": {
                "icebergUnreferencedFileRemoval": { "unreferencedDays": 3, "nonCurrentDays": 10 }
            }
        }),
    );
    m
}

/// The default maintenance configuration AWS returns for a freshly created
/// table: compaction + snapshot management enabled, keyed by the
/// `TableMaintenanceType` enum values (`icebergCompaction`,
/// `icebergSnapshotManagement`).
fn default_table_maintenance() -> std::collections::BTreeMap<String, Value> {
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        "icebergCompaction".to_string(),
        json!({
            "status": "enabled",
            "settings": {
                "icebergCompaction": { "targetFileSizeMB": 512, "strategy": "binpack" }
            }
        }),
    );
    m.insert(
        "icebergSnapshotManagement".to_string(),
        json!({
            "status": "enabled",
            "settings": {
                "icebergSnapshotManagement": { "minSnapshotsToKeep": 1, "maxSnapshotAgeHours": 120 }
            }
        }),
    );
    m
}

/// The warehouse S3 location for a table (opaque; no bytes are stored). AWS
/// derives it from the table's own id: `s3://<table-id>--table-s3`.
fn warehouse_location(table_id: &str) -> String {
    format!("s3://{table_id}--table-s3")
}

/// Offset pagination over a sorted key list. Lenient: an unparseable
/// continuation token restarts from the beginning rather than erroring, so
/// positive variants that pass a synthetic token still succeed.
fn paginate(
    total: usize,
    token: Option<&str>,
    max: Option<usize>,
) -> (usize, usize, Option<String>) {
    let start = token
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0)
        .min(total);
    let limit = max.filter(|n| *n > 0).unwrap_or(1000);
    let end = (start + limit).min(total);
    let next = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    (start, end, next)
}

fn query_usize(req: &AwsRequest, key: &str) -> Option<usize> {
    req.query_params
        .get(key)
        .and_then(|s| s.parse::<usize>().ok())
}

// ---------------------------------------------------------------------------
// Value builders
// ---------------------------------------------------------------------------

fn bucket_summary(b: &TableBucketRecord) -> Value {
    json!({
        "arn": b.arn,
        "name": b.name,
        "ownerAccountId": b.owner_account_id,
        "createdAt": ts(b.created_at),
        "tableBucketId": b.table_bucket_id,
        "type": b.bucket_type,
    })
}

fn namespace_summary(n: &NamespaceRecord, table_bucket_id: &str) -> Value {
    json!({
        "namespace": [n.name],
        "createdAt": ts(n.created_at),
        "createdBy": n.created_by,
        "ownerAccountId": n.owner_account_id,
        "namespaceId": n.namespace_id,
        "tableBucketId": table_bucket_id,
    })
}

fn table_summary(t: &TableRecord, table_bucket_id: &str) -> Value {
    let mut m = Map::new();
    m.insert("namespace".into(), json!([t.namespace]));
    m.insert("name".into(), json!(t.name));
    m.insert("type".into(), json!(t.table_type));
    m.insert("tableARN".into(), json!(t.arn));
    m.insert("createdAt".into(), ts(t.created_at));
    m.insert("modifiedAt".into(), ts(t.modified_at));
    if let Some(mbs) = &t.managed_by_service {
        m.insert("managedByService".into(), json!(mbs));
    }
    m.insert("namespaceId".into(), json!(t.namespace_id));
    m.insert("tableBucketId".into(), json!(table_bucket_id));
    Value::Object(m)
}

impl S3TablesService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        action: &str,
        l: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate::validate(action, req)?;
        match action {
            // buckets
            "CreateTableBucket" => self.create_table_bucket(req),
            "ListTableBuckets" => self.list_table_buckets(req),
            "GetTableBucket" => self.get_table_bucket(req, &l[0]),
            "DeleteTableBucket" => self.delete_table_bucket(req, &l[0]),
            "GetTableBucketEncryption" => self.get_bucket_sub(req, &l[0], SubKind::Encryption),
            "PutTableBucketEncryption" => self.put_bucket_sub(req, &l[0], SubKind::Encryption),
            "DeleteTableBucketEncryption" => {
                self.delete_bucket_sub(req, &l[0], SubKind::Encryption)
            }
            "GetTableBucketMaintenanceConfiguration" => self.get_bucket_maintenance(req, &l[0]),
            "PutTableBucketMaintenanceConfiguration" => {
                self.put_bucket_maintenance(req, &l[0], &l[1])
            }
            "GetTableBucketMetricsConfiguration" => self.get_bucket_metrics(req, &l[0]),
            "PutTableBucketMetricsConfiguration" => self.put_bucket_metrics(req, &l[0]),
            "DeleteTableBucketMetricsConfiguration" => {
                self.delete_bucket_sub(req, &l[0], SubKind::Metrics)
            }
            "GetTableBucketPolicy" => self.get_bucket_sub(req, &l[0], SubKind::Policy),
            "PutTableBucketPolicy" => self.put_bucket_sub(req, &l[0], SubKind::Policy),
            "DeleteTableBucketPolicy" => self.delete_bucket_sub(req, &l[0], SubKind::Policy),
            "GetTableBucketStorageClass" => self.get_bucket_sub(req, &l[0], SubKind::StorageClass),
            "PutTableBucketStorageClass" => self.put_bucket_sub(req, &l[0], SubKind::StorageClass),
            "GetTableBucketReplication" => self.get_bucket_replication(req),
            "PutTableBucketReplication" => self.put_bucket_replication(req),
            "DeleteTableBucketReplication" => self.delete_bucket_replication(req),
            // namespaces
            "CreateNamespace" => self.create_namespace(req, &l[0]),
            "ListNamespaces" => self.list_namespaces(req, &l[0]),
            "GetNamespace" => self.get_namespace(req, &l[0], &l[1]),
            "DeleteNamespace" => self.delete_namespace(req, &l[0], &l[1]),
            // tables
            "CreateTable" => self.create_table(req, &l[0], &l[1]),
            "ListTables" => self.list_tables(req, &l[0]),
            "GetTable" => self.get_table(req, l),
            "DeleteTable" => self.delete_table(req, &l[0], &l[1], &l[2]),
            "RenameTable" => self.rename_table(req, &l[0], &l[1], &l[2]),
            "UpdateTableMetadataLocation" => self.update_table_metadata(req, &l[0], &l[1], &l[2]),
            "GetTableMetadataLocation" => self.get_table_metadata(req, &l[0], &l[1], &l[2]),
            "GetTableEncryption" => {
                self.get_table_sub(req, &l[0], &l[1], &l[2], SubKind::Encryption)
            }
            "GetTableStorageClass" => {
                self.get_table_sub(req, &l[0], &l[1], &l[2], SubKind::StorageClass)
            }
            "GetTablePolicy" => self.get_table_sub(req, &l[0], &l[1], &l[2], SubKind::Policy),
            "PutTablePolicy" => self.put_table_sub(req, &l[0], &l[1], &l[2], SubKind::Policy),
            "DeleteTablePolicy" => self.delete_table_sub(req, &l[0], &l[1], &l[2], SubKind::Policy),
            "GetTableMaintenanceConfiguration" => {
                self.get_table_maintenance(req, &l[0], &l[1], &l[2])
            }
            "PutTableMaintenanceConfiguration" => {
                self.put_table_maintenance(req, &l[0], &l[1], &l[2], &l[3])
            }
            "GetTableMaintenanceJobStatus" => {
                self.get_table_maintenance_job(req, &l[0], &l[1], &l[2])
            }
            // table replication (query form)
            "GetTableReplication" => self.get_table_replication(req),
            "PutTableReplication" => self.put_table_replication(req),
            "DeleteTableReplication" => self.delete_table_replication(req),
            "GetTableReplicationStatus" => self.get_table_replication_status(req),
            // record expiration (query form)
            "GetTableRecordExpirationConfiguration" => self.get_record_expiration(req),
            "PutTableRecordExpirationConfiguration" => self.put_record_expiration(req),
            "GetTableRecordExpirationJobStatus" => self.get_record_expiration_job(req),
            // tagging
            "ListTagsForResource" => self.list_tags(req, &l[0]),
            "TagResource" => self.tag_resource(req, &l[0]),
            "UntagResource" => self.untag_resource(req, &l[0]),
            _ => Err(AwsServiceError::action_not_implemented("s3tables", action)),
        }
    }
}

/// Simple opaque sub-resources shared between bucket and table.
#[derive(Clone, Copy)]
enum SubKind {
    Encryption,
    Policy,
    StorageClass,
    Metrics,
}

include!("handlers.rs");
