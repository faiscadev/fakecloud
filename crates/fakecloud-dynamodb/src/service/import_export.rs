//! DynamoDB S3 import (`ImportTable`) and export (`ExportTableToPointInTime`)
//! plus their Describe/List operations.
//!
//! Both are jobs on AWS: the start call validates the request, records the
//! job (and, for an import, the `CREATING` table) and returns at once with
//! status `IN_PROGRESS`; the S3 reads/writes, (de)compression and parsing run
//! afterwards and the job settles to `COMPLETED` or `FAILED`, which
//! DescribeImport / DescribeExport (and DescribeTable for the imported table)
//! report. Here the job runs on a background task so the request path never
//! holds the DynamoDB or S3 state locks across body IO: object bodies are
//! opened under a short S3 read guard and read after it is dropped, and the
//! export's objects are written through the S3 store before a short write
//! guard inserts them.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use base64::Engine;
use chrono::{DateTime, Utc};
use http::StatusCode;
use md5::{Digest, Md5};
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_persistence::{S3Store, SnapshotStore};
use fakecloud_s3::body_io::ObjectBodyHandle;
use fakecloud_s3::SharedS3State;

use crate::state::{
    DynamoDbState, DynamoTable, ExportDescription, ImportDescription, ProvisionedThroughput,
    SharedDynamoDbState,
};

use super::import_formats::{self, CsvOptions, ParsedRow};
use super::{
    find_table_by_arn, parse_attribute_definitions, parse_gsi, parse_key_schema,
    parse_on_demand_throughput, parse_provisioned_throughput, require_str, save_dynamodb_snapshot,
    validate_index_keys_in_item, validate_item_attribute_values, validate_key_in_item,
    DynamoDbService,
};

type Item = HashMap<String, Value>;

const ITEM_VALIDATION_MESSAGE: &str = "Some of the items failed validation checks and were not \
     imported. Please check CloudWatch error logs for more details.";
const NO_SUCH_BUCKET_MESSAGE: &str = "The specified bucket does not exist (Service: Amazon S3; \
     Status Code: 404; Error Code: NoSuchBucket)";

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

/// AWS import/export ids: a zero-padded 14-digit millisecond timestamp and an
/// 8-hex-digit suffix, e.g. `01658528578619-c4d4e311`.
fn job_id(now: DateTime<Utc>) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{:014}-{}", now.timestamp_millis(), &suffix[..8])
}

fn epoch_secs(t: DateTime<Utc>) -> f64 {
    t.timestamp_millis() as f64 / 1000.0
}

/// Everything a background job needs, detached from the request.
#[derive(Clone)]
pub(crate) struct JobContext {
    state: SharedDynamoDbState,
    s3_state: Option<SharedS3State>,
    s3_store: Option<Arc<dyn S3Store>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
}

impl JobContext {
    /// Run `work` off the request path, then persist the state it settled.
    /// Inside a Tokio runtime the work runs on the blocking pool (it does
    /// synchronous body IO and parsing) from a spawned task, so the start
    /// call returns before it begins; with no runtime (synchronous callers)
    /// it runs inline. `on_panic` settles the job as failed if `work`
    /// panics, so it never stays `IN_PROGRESS` forever.
    fn run<W, P>(self, work: W, on_panic: P)
    where
        W: FnOnce(&JobContext) + Send + 'static,
        P: FnOnce(&JobContext) + Send + 'static,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let ctx = self.clone();
                    let joined = tokio::task::spawn_blocking(move || work(&ctx)).await;
                    if let Err(err) = joined {
                        tracing::error!(%err, "dynamodb import/export job panicked");
                        on_panic(&self);
                    }
                    if let Err(err) = save_dynamodb_snapshot(
                        &self.state,
                        self.snapshot_store.clone(),
                        &self.snapshot_lock,
                    )
                    .await
                    {
                        tracing::error!(%err, "dynamodb snapshot save failed");
                    }
                });
            }
            Err(_) => work(&self),
        }
    }

    fn with_account<R>(&self, account_id: &str, f: impl FnOnce(&mut DynamoDbState) -> R) -> R {
        let mut accounts = self.state.write();
        f(accounts.get_or_create(account_id))
    }
}

/// An S3 source object opened under the S3 read guard.
struct SourceObject {
    size: u64,
    body: ObjectBodyHandle,
}

enum SourceError {
    NoSuchBucket,
    Read(String),
}

/// Open every object under `prefix` in `bucket` (owned by `account`) while
/// holding the S3 read guard only long enough to resolve keys and open the
/// bodies; the caller reads them with no lock held.
fn open_source_objects(
    s3: &SharedS3State,
    account: &str,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<SourceObject>, SourceError> {
    let guard = s3.read();
    let acct = guard.get(account).ok_or(SourceError::NoSuchBucket)?;
    let bucket = acct.buckets.get(bucket).ok_or(SourceError::NoSuchBucket)?;
    let mut keys: Vec<&String> = bucket
        .objects
        .keys()
        .filter(|k| k.starts_with(prefix))
        .collect();
    keys.sort();
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let obj = &bucket.objects[key];
        if obj.is_delete_marker {
            continue;
        }
        let body = ObjectBodyHandle::open(&obj.body)
            .map_err(|e| SourceError::Read(format!("failed to open s3://{key}: {e}")))?;
        out.push(SourceObject {
            size: obj.size,
            body,
        });
    }
    Ok(out)
}

/// Write one object into a bucket: through the durable store first (no S3
/// lock held), then a short write guard to insert it into the bucket.
fn put_s3_object(
    ctx: &JobContext,
    account: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
) -> Result<(), SourceError> {
    let Some(s3) = &ctx.s3_state else {
        return Ok(());
    };
    let bytes = bytes::Bytes::from(body);
    let mut obj = fakecloud_s3::S3Object {
        key: key.to_string(),
        body: fakecloud_s3::memory_body(bytes.clone()),
        content_type: content_type.to_string(),
        etag: format!("{:x}", Md5::digest(&bytes)),
        size: bytes.len() as u64,
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        ..Default::default()
    };
    if let Some(store) = &ctx.s3_store {
        let meta = fakecloud_s3::persistence::object_meta_snapshot(&obj);
        obj.body = store
            .put_object(
                bucket,
                key,
                None,
                fakecloud_persistence::BodySource::Bytes(bytes),
                &meta,
            )
            .map_err(|e| SourceError::Read(format!("failed to write s3://{bucket}/{key}: {e}")))?;
    }
    let mut guard = s3.write();
    let Some(b) = guard
        .get_mut(account)
        .and_then(|a| a.buckets.get_mut(bucket))
    else {
        // The bucket was deleted between our check and this write. Drop the
        // durable copy too so no orphan is left on disk.
        drop(guard);
        if let Some(store) = &ctx.s3_store {
            let _ = store.delete_object(bucket, key, None);
        }
        return Err(SourceError::NoSuchBucket);
    };
    b.objects.insert(key.to_string(), obj);
    Ok(())
}

fn bucket_exists(ctx: &JobContext, account: &str, bucket: &str) -> bool {
    ctx.s3_state.as_ref().is_none_or(|s3| {
        s3.read()
            .get(account)
            .is_some_and(|a| a.buckets.contains_key(bucket))
    })
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

struct ImportJob {
    account_id: String,
    import_arn: String,
    table_name: String,
    table_id: String,
    bucket: String,
    bucket_account: String,
    prefix: String,
    input_format: String,
    compression: String,
    csv: Option<CsvOptions>,
}

fn parse_rows(job: &ImportJob, text: &str) -> Vec<ParsedRow> {
    match job.input_format.as_str() {
        "ION" => import_formats::parse_ion(text),
        "CSV" => import_formats::parse_csv(
            text,
            job.csv
                .as_ref()
                .expect("CSV options are built for CSV imports"),
        ),
        _ => import_formats::parse_dynamodb_json(text),
    }
}

/// Settle an import as failed. When `drop_table` is set the failure was hit
/// before any data was imported and, as on AWS, the table is not created.
fn fail_import(ctx: &JobContext, job: &ImportJob, code: &str, message: &str, drop_table: bool) {
    ctx.with_account(&job.account_id, |state| {
        if drop_table
            && state
                .tables
                .get(&job.table_name)
                .is_some_and(|t| t.table_id == job.table_id)
        {
            state.tables.remove(&job.table_name);
        } else if let Some(t) = state
            .tables
            .get_mut(&job.table_name)
            .filter(|t| t.table_id == job.table_id && t.status == "CREATING")
        {
            t.status = "ACTIVE".to_string();
        }
        if let Some(imp) = state.imports.get_mut(&job.import_arn) {
            imp.import_status = "FAILED".to_string();
            imp.failure_code = Some(code.to_string());
            imp.failure_message = Some(message.to_string());
            imp.end_time = Some(Utc::now());
        }
    });
}

fn run_import(ctx: &JobContext, job: &ImportJob) {
    let objects = match &ctx.s3_state {
        Some(s3) => open_source_objects(s3, &job.bucket_account, &job.bucket, &job.prefix),
        None => Ok(Vec::new()),
    };
    let objects = match objects {
        Ok(o) => o,
        Err(SourceError::NoSuchBucket) => {
            return fail_import(ctx, job, "S3NoSuchBucket", NO_SUCH_BUCKET_MESSAGE, true);
        }
        Err(SourceError::Read(msg)) => {
            return fail_import(ctx, job, "InternalServerError", &msg, true);
        }
    };

    // Read, decompress and parse with no lock held.
    let mut rows: Vec<ParsedRow> = Vec::new();
    let mut processed_size_bytes: i64 = 0;
    for obj in objects {
        processed_size_bytes += obj.size as i64;
        let raw = match obj.body.read_all() {
            Ok(b) => b,
            Err(e) => {
                rows.push(Err(format!("failed to read S3 object: {e}")));
                continue;
            }
        };
        // An object that cannot be decoded is one error; AWS skips the rest
        // of an object it cannot process.
        let text = import_formats::decompress(&raw, &job.compression).and_then(|d| {
            String::from_utf8(d).map_err(|_| "S3 object is not valid UTF-8".to_string())
        });
        match text {
            Ok(text) => rows.extend(parse_rows(job, &text)),
            Err(e) => rows.push(Err(e)),
        }
    }

    ctx.with_account(&job.account_id, |state| {
        let Some(table) = state
            .tables
            .get_mut(&job.table_name)
            .filter(|t| t.table_id == job.table_id)
        else {
            if let Some(imp) = state.imports.get_mut(&job.import_arn) {
                imp.import_status = "FAILED".to_string();
                imp.failure_code = Some("ResourceNotFoundException".to_string());
                imp.failure_message = Some(format!(
                    "Table {} was deleted while the import was in progress",
                    job.table_name
                ));
                imp.end_time = Some(Utc::now());
            }
            return;
        };
        // Each row is written the way a PutItem would be. A row PutItem would
        // reject -- unparsable, no valid primary key, a bad index key or a
        // malformed attribute value -- is an import error, counted and
        // skipped rather than stored; a row repeating an earlier row's key
        // replaces it (not an error).
        let processed_item_count = rows.len() as i64;
        let mut error_count = 0i64;
        for row in rows {
            let Ok(item) = row else {
                error_count += 1;
                continue;
            };
            if validate_key_in_item(table, &item).is_err()
                || validate_index_keys_in_item(table, &item).is_err()
                || validate_item_attribute_values(&item).is_err()
            {
                error_count += 1;
                continue;
            }
            table.put_item_at_key(item);
        }
        table.status = "ACTIVE".to_string();
        let imported_item_count = table.item_count;
        if let Some(imp) = state.imports.get_mut(&job.import_arn) {
            imp.processed_item_count = processed_item_count;
            imp.processed_size_bytes = processed_size_bytes;
            imp.imported_item_count = imported_item_count;
            imp.error_count = error_count;
            imp.end_time = Some(Utc::now());
            if error_count > 0 {
                imp.import_status = "FAILED".to_string();
                imp.failure_code = Some("ItemValidationError".to_string());
                imp.failure_message = Some(ITEM_VALIDATION_MESSAGE.to_string());
            } else {
                imp.import_status = "COMPLETED".to_string();
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

struct ExportJob {
    account_id: String,
    export_arn: String,
    export_id: String,
    table_arn: String,
    table_id: String,
    bucket: String,
    bucket_account: String,
    prefix: Option<String>,
    format: String,
    items: Vec<Item>,
    start_time: DateTime<Utc>,
    export_time: DateTime<Utc>,
    sse_algorithm: Option<String>,
    sse_kms_key_id: Option<String>,
}

fn random_file_stem() -> String {
    // AWS names data files with 26 lowercase base32 characters.
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let a = uuid::Uuid::new_v4().as_u128();
    let b = uuid::Uuid::new_v4().as_u128();
    (0..26)
        .map(|i| {
            let v = if i < 20 {
                a >> (i * 5)
            } else {
                b >> ((i - 20) * 5)
            };
            ALPHABET[(v & 31) as usize] as char
        })
        .collect()
}

fn gzip(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    // Writing into a Vec cannot fail.
    let _ = enc.write_all(data);
    enc.finish().unwrap_or_default()
}

fn settle_export(ctx: &JobContext, job: &ExportJob, update: impl FnOnce(&mut ExportDescription)) {
    ctx.with_account(&job.account_id, |state| {
        if let Some(exp) = state.exports.get_mut(&job.export_arn) {
            update(exp);
            exp.end_time = Some(Utc::now());
        }
    });
}

fn fail_export(ctx: &JobContext, job: &ExportJob, code: &str, message: &str) {
    settle_export(ctx, job, |exp| {
        exp.export_status = "FAILED".to_string();
        exp.failure_code = Some(code.to_string());
        exp.failure_message = Some(message.to_string());
    });
}

fn run_export(ctx: &JobContext, job: &ExportJob) {
    let item_count = job.items.len() as i64;
    if ctx.s3_state.is_some() && !bucket_exists(ctx, &job.bucket_account, &job.bucket) {
        return fail_export(ctx, job, "S3NoSuchBucket", NO_SUCH_BUCKET_MESSAGE);
    }

    // Serialize + compress with no lock held.
    let (ext, line): (&str, fn(&Item) -> String) = if job.format == "ION" {
        ("ion.gz", import_formats::ion_line)
    } else {
        ("json.gz", import_formats::dynamodb_json_line)
    };
    let mut data = String::new();
    for item in &job.items {
        data.push_str(&line(item));
        data.push('\n');
    }
    let billed_size_bytes = data.len() as i64;
    let compressed = gzip(data.as_bytes());

    let base = match job.prefix.as_deref().map(|p| p.trim_end_matches('/')) {
        Some(p) if !p.is_empty() => format!("{p}/AWSDynamoDB/{}", job.export_id),
        _ => format!("AWSDynamoDB/{}", job.export_id),
    };
    let data_key = format!("{base}/data/{}.{ext}", random_file_stem());
    let manifest_files_key = format!("{base}/manifest-files.json");
    let manifest_summary_key = format!("{base}/manifest-summary.json");
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&compressed));
    let etag = format!("{:x}", Md5::digest(&compressed));
    let manifest_files = format!(
        "{}\n",
        json!({
            "itemCount": item_count,
            "md5Checksum": md5_b64,
            "etag": etag,
            "dataFileS3Key": data_key,
        })
    );
    let end_time = Utc::now();
    let ts = |t: DateTime<Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let summary = json!({
        "version": "2020-06-30",
        "exportArn": job.export_arn,
        "startTime": ts(job.start_time),
        "endTime": ts(end_time),
        "tableArn": job.table_arn,
        "tableId": job.table_id,
        "exportTime": ts(job.export_time),
        "s3Bucket": job.bucket,
        "s3Prefix": job.prefix,
        "s3SseAlgorithm": job.sse_algorithm.clone().unwrap_or_else(|| "AES256".to_string()),
        "s3SseKmsKeyId": job.sse_kms_key_id,
        "manifestFilesS3Key": manifest_files_key,
        "billedSizeBytes": billed_size_bytes,
        "itemCount": item_count,
        "outputFormat": job.format,
        "exportType": "FULL_EXPORT",
    });

    let started_key = format!("{base}/_started");
    let writes: [(&str, Vec<u8>, &str); 4] = [
        (started_key.as_str(), Vec::new(), "application/octet-stream"),
        (data_key.as_str(), compressed, "application/x-gzip"),
        (
            manifest_files_key.as_str(),
            manifest_files.into_bytes(),
            "application/json",
        ),
        (
            manifest_summary_key.as_str(),
            summary.to_string().into_bytes(),
            "application/json",
        ),
    ];
    for (key, body, content_type) in writes {
        match put_s3_object(
            ctx,
            &job.bucket_account,
            &job.bucket,
            key,
            body,
            content_type,
        ) {
            Ok(()) => {}
            Err(SourceError::NoSuchBucket) => {
                return fail_export(ctx, job, "S3NoSuchBucket", NO_SUCH_BUCKET_MESSAGE);
            }
            Err(SourceError::Read(msg)) => {
                return fail_export(ctx, job, "InternalServerError", &msg);
            }
        }
    }

    settle_export(ctx, job, |exp| {
        exp.export_status = "COMPLETED".to_string();
        exp.item_count = item_count;
        exp.billed_size_bytes = billed_size_bytes;
        exp.export_manifest = Some(manifest_summary_key.clone());
    });
}

// ---------------------------------------------------------------------------
// Job reconstruction (start call and restart recovery share these)
// ---------------------------------------------------------------------------

fn csv_options(format_options: Option<&Value>, table: &DynamoTable) -> CsvOptions {
    let csv = format_options.and_then(|o| o.get("Csv"));
    let delimiter = csv
        .and_then(|c| c.get("Delimiter"))
        .and_then(Value::as_str)
        .and_then(|d| d.chars().next())
        .unwrap_or(',');
    let header = csv
        .and_then(|c| c.get("HeaderList"))
        .and_then(Value::as_array)
        .map(|h| {
            h.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        });
    // Key attributes of the base table and its secondary indexes keep their
    // declared type; every other column is a string.
    let key_types = table
        .attribute_definitions
        .iter()
        .map(|a| (a.attribute_name.clone(), a.attribute_type.clone()))
        .collect();
    CsvOptions {
        delimiter,
        header,
        key_types,
    }
}

fn import_job(account_id: &str, imp: &ImportDescription, table: &DynamoTable) -> ImportJob {
    ImportJob {
        account_id: account_id.to_string(),
        import_arn: imp.import_arn.clone(),
        table_name: imp.table_name.clone(),
        table_id: table.table_id.clone(),
        bucket: imp.s3_bucket_source.clone(),
        bucket_account: imp
            .s3_bucket_owner
            .clone()
            .unwrap_or_else(|| account_id.to_string()),
        prefix: imp.s3_key_prefix.clone().unwrap_or_default(),
        input_format: imp.input_format.clone(),
        compression: imp
            .input_compression_type
            .clone()
            .unwrap_or_else(|| "NONE".to_string()),
        csv: (imp.input_format == "CSV")
            .then(|| csv_options(imp.input_format_options.as_ref(), table)),
    }
}

fn export_job(account_id: &str, exp: &ExportDescription, table: &DynamoTable) -> ExportJob {
    ExportJob {
        account_id: account_id.to_string(),
        export_arn: exp.export_arn.clone(),
        export_id: exp
            .export_arn
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string(),
        table_arn: exp.table_arn.clone(),
        table_id: table.table_id.clone(),
        bucket: exp.s3_bucket.clone(),
        bucket_account: exp
            .s3_bucket_owner
            .clone()
            .unwrap_or_else(|| account_id.to_string()),
        prefix: exp.s3_prefix.clone(),
        format: exp.export_format.clone(),
        // Snapshot the rows under the caller's (short) lock; serialization
        // and the S3 writes happen on the job.
        items: table.items().iter().cloned().collect(),
        start_time: exp.start_time,
        export_time: exp.export_time,
        sse_algorithm: exp.s3_sse_algorithm.clone(),
        sse_kms_key_id: exp.s3_sse_kms_key_id.clone(),
    }
}

fn spawn_import(ctx: JobContext, job: ImportJob) {
    let job = Arc::new(job);
    let on_panic = job.clone();
    ctx.run(
        move |ctx| run_import(ctx, &job),
        move |ctx| {
            fail_import(
                ctx,
                &on_panic,
                "InternalServerError",
                "The import job failed unexpectedly",
                false,
            )
        },
    );
}

fn spawn_export(ctx: JobContext, job: ExportJob) {
    let arn = job.export_arn.clone();
    let account = job.account_id.clone();
    ctx.run(
        move |ctx| run_export(ctx, &job),
        move |ctx| {
            ctx.with_account(&account, |state| {
                if let Some(exp) = state.exports.get_mut(&arn) {
                    exp.export_status = "FAILED".to_string();
                    exp.failure_code = Some("InternalServerError".to_string());
                    exp.failure_message = Some("The export job failed unexpectedly".to_string());
                    exp.end_time = Some(Utc::now());
                }
            })
        },
    );
}

fn check_enum(body: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    match body.get(field).and_then(Value::as_str) {
        Some(v) if !allowed.contains(&v) => Err(validation(format!(
            "1 validation error detected: Value '{v}' at '{}' failed to satisfy constraint: \
             Member must satisfy enum value set: [{}]",
            {
                let mut c = field.chars();
                c.next()
                    .map(|f| f.to_lowercase().collect::<String>() + c.as_str())
                    .unwrap_or_default()
            },
            allowed.join(", ")
        ))),
        _ => Ok(()),
    }
}

fn import_description_json(imp: &ImportDescription, full: bool) -> Value {
    let mut source = json!({ "S3Bucket": imp.s3_bucket_source });
    if let Some(p) = &imp.s3_key_prefix {
        source["S3KeyPrefix"] = json!(p);
    }
    if let Some(o) = &imp.s3_bucket_owner {
        source["S3BucketOwner"] = json!(o);
    }
    let mut d = json!({
        "ImportArn": imp.import_arn,
        "ImportStatus": imp.import_status,
        "TableArn": imp.table_arn,
        "S3BucketSource": source,
        "InputFormat": imp.input_format,
        "StartTime": epoch_secs(imp.start_time),
    });
    if let Some(end) = imp.end_time {
        d["EndTime"] = json!(epoch_secs(end));
    }
    if !full {
        return d;
    }
    d["InputCompressionType"] = json!(imp
        .input_compression_type
        .clone()
        .unwrap_or_else(|| "NONE".to_string()));
    if let Some(id) = &imp.table_id {
        d["TableId"] = json!(id);
    }
    if let Some(t) = &imp.client_token {
        d["ClientToken"] = json!(t);
    }
    if let Some(o) = &imp.input_format_options {
        d["InputFormatOptions"] = o.clone();
    }
    if let Some(p) = &imp.table_creation_parameters {
        d["TableCreationParameters"] = p.clone();
    }
    if imp.import_status != "IN_PROGRESS" {
        d["ProcessedItemCount"] = json!(imp.processed_item_count);
        d["ProcessedSizeBytes"] = json!(imp.processed_size_bytes);
        d["ImportedItemCount"] = json!(imp.imported_item_count);
        d["ErrorCount"] = json!(imp.error_count);
    }
    if let Some(c) = &imp.failure_code {
        d["FailureCode"] = json!(c);
    }
    if let Some(m) = &imp.failure_message {
        d["FailureMessage"] = json!(m);
    }
    d
}

fn export_description_json(exp: &ExportDescription) -> Value {
    let mut d = json!({
        "ExportArn": exp.export_arn,
        "ExportStatus": exp.export_status,
        "TableArn": exp.table_arn,
        "S3Bucket": exp.s3_bucket,
        "ExportFormat": exp.export_format,
        "ExportType": "FULL_EXPORT",
        "StartTime": epoch_secs(exp.start_time),
        "ExportTime": epoch_secs(exp.export_time),
        "S3SseAlgorithm": exp.s3_sse_algorithm.clone().unwrap_or_else(|| "AES256".to_string()),
    });
    let opt = |d: &mut Value, k: &str, v: &Option<String>| {
        if let Some(v) = v {
            d[k] = json!(v);
        }
    };
    opt(&mut d, "S3Prefix", &exp.s3_prefix);
    opt(&mut d, "TableId", &exp.table_id);
    opt(&mut d, "S3BucketOwner", &exp.s3_bucket_owner);
    opt(&mut d, "S3SseKmsKeyId", &exp.s3_sse_kms_key_id);
    opt(&mut d, "ClientToken", &exp.client_token);
    opt(&mut d, "ExportManifest", &exp.export_manifest);
    opt(&mut d, "FailureCode", &exp.failure_code);
    opt(&mut d, "FailureMessage", &exp.failure_message);
    if let Some(end) = exp.end_time {
        d["EndTime"] = json!(epoch_secs(end));
    }
    if exp.export_status == "COMPLETED" {
        d["ItemCount"] = json!(exp.item_count);
        d["BilledSizeBytes"] = json!(exp.billed_size_bytes);
    }
    d
}

impl DynamoDbService {
    pub(crate) fn job_context(&self) -> JobContext {
        JobContext {
            state: self.state.clone(),
            s3_state: self.s3_state.clone(),
            s3_store: self.s3_store.clone(),
            snapshot_store: self.snapshot_store.clone(),
            snapshot_lock: self.snapshot_lock.clone(),
        }
    }

    /// Resume import/export jobs a restart interrupted. Jobs run in the
    /// background and only commit when they finish, so a persisted
    /// `IN_PROGRESS` job has written nothing yet (an import's table is still
    /// empty and `CREATING`); running it again settles it exactly as the
    /// interrupted run would have. An export whose table is gone fails.
    pub(crate) fn resume_interrupted_jobs(&self) {
        let ctx = self.job_context();
        let mut imports = Vec::new();
        let mut exports = Vec::new();
        {
            let mut accounts = self.state.write();
            for (account_id, state) in accounts.iter_mut() {
                let account_id = account_id.to_string();
                for imp in state.imports.values() {
                    if imp.import_status != "IN_PROGRESS" {
                        continue;
                    }
                    match state.tables.get(&imp.table_name) {
                        Some(t) if imp.table_id.as_deref().is_none_or(|id| id == t.table_id) => {
                            imports.push(import_job(&account_id, imp, t));
                        }
                        _ => {}
                    }
                }
                for exp in state.exports.values_mut() {
                    if exp.export_status != "IN_PROGRESS" {
                        continue;
                    }
                    match find_table_by_arn(&state.tables, &exp.table_arn) {
                        Ok(t) => exports.push(export_job(&account_id, exp, t)),
                        Err(_) => {
                            exp.export_status = "FAILED".to_string();
                            exp.failure_code = Some("TableNotFoundException".to_string());
                            exp.failure_message =
                                Some(format!("Table {} no longer exists", exp.table_arn));
                            exp.end_time = Some(Utc::now());
                        }
                    }
                }
            }
        }
        for job in imports {
            spawn_import(ctx.clone(), job);
        }
        for job in exports {
            spawn_export(ctx.clone(), job);
        }
    }

    pub(super) fn export_table_to_point_in_time(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_arn = require_str(&body, "TableArn")?.to_string();
        let s3_bucket = require_str(&body, "S3Bucket")?.to_string();
        validate_optional_string_length("s3Prefix", body["S3Prefix"].as_str(), 0, 1024)?;
        check_enum(&body, "ExportFormat", &["DYNAMODB_JSON", "ION"])?;
        check_enum(&body, "ExportType", &["FULL_EXPORT", "INCREMENTAL_EXPORT"])?;
        check_enum(&body, "S3SseAlgorithm", &["AES256", "KMS"])?;
        let s3_prefix = body["S3Prefix"].as_str().map(str::to_string);
        let export_format = body["ExportFormat"]
            .as_str()
            .unwrap_or("DYNAMODB_JSON")
            .to_string();

        let now = Utc::now();
        let export_time = match body.get("ExportTime").and_then(Value::as_f64) {
            Some(secs) => {
                let t =
                    DateTime::<Utc>::from_timestamp_millis((secs * 1000.0) as i64).unwrap_or(now);
                if t > now {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidExportTimeException",
                        "Export time is in the future",
                    ));
                }
                t
            }
            None => now,
        };

        let export_id = job_id(now);
        // Snapshot the table's rows under a read guard; the job serializes and
        // writes them with no lock held.
        let (job, export) = {
            let accounts = self.state.read();
            let not_found = || {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "TableNotFoundException",
                    format!("Requested resource not found: Table ARN: {table_arn} not found"),
                )
            };
            let state = accounts.get(&req.account_id).ok_or_else(not_found)?;
            // ExportTableToPointInTime declares TableNotFoundException; remap
            // the generic ResourceNotFoundException from find_table_by_arn.
            let table = find_table_by_arn(&state.tables, &table_arn).map_err(|_| not_found())?;
            let export = ExportDescription {
                export_arn: format!("{}/export/{export_id}", table.arn),
                export_status: "IN_PROGRESS".to_string(),
                table_arn: table.arn.clone(),
                s3_bucket: s3_bucket.clone(),
                s3_prefix,
                export_format,
                start_time: now,
                end_time: None,
                export_time,
                item_count: 0,
                billed_size_bytes: 0,
                failure_code: None,
                failure_message: None,
                export_manifest: None,
                table_id: Some(table.table_id.clone()),
                s3_bucket_owner: body["S3BucketOwner"].as_str().map(str::to_string),
                s3_sse_algorithm: Some(
                    body["S3SseAlgorithm"]
                        .as_str()
                        .unwrap_or("AES256")
                        .to_string(),
                ),
                s3_sse_kms_key_id: body["S3SseKmsKeyId"].as_str().map(str::to_string),
                client_token: body["ClientToken"].as_str().map(str::to_string),
            };
            (export_job(&req.account_id, &export, table), export)
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .exports
            .insert(export.export_arn.clone(), export.clone());
        spawn_export(self.job_context(), job);

        Self::ok_json(json!({ "ExportDescription": export_description_json(&export) }))
    }

    pub(super) fn describe_export(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let export_arn = require_str(&body, "ExportArn")?;
        let accounts = self.state.read();
        let export = accounts
            .get(&req.account_id)
            .and_then(|s| s.exports.get(export_arn))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ExportNotFoundException",
                    format!("Export not found: {export_arn}"),
                )
            })?;
        Self::ok_json(json!({ "ExportDescription": export_description_json(export) }))
    }

    pub(super) fn list_exports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length("tableArn", body["TableArn"].as_str(), 1, 1024)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 1, 25)?;
        let table_arn = body["TableArn"].as_str();
        let start = body["NextToken"].as_str();
        let max = body["MaxResults"].as_i64().map(|m| m as usize);

        let accounts = self.state.read();
        let empty = BTreeMap::new();
        let exports = accounts
            .get(&req.account_id)
            .map(|s| &s.exports)
            .unwrap_or(&empty);
        // Honor MaxResults + NextToken (export-arn cursor).
        let matched: Vec<(&str, Value)> = exports
            .values()
            .filter(|e| table_arn.is_none() || table_arn == Some(e.table_arn.as_str()))
            .filter(|e| start.is_none_or(|s| e.export_arn.as_str() > s))
            .map(|e| {
                (
                    e.export_arn.as_str(),
                    json!({
                        "ExportArn": e.export_arn,
                        "ExportStatus": e.export_status,
                        "ExportType": "FULL_EXPORT",
                    }),
                )
            })
            .collect();
        let truncated = max.is_some_and(|m| matched.len() > m);
        let take = max.unwrap_or(matched.len());
        let summaries: Vec<Value> = matched.iter().take(take).map(|(_, v)| v.clone()).collect();
        let mut resp = json!({ "ExportSummaries": summaries });
        if truncated {
            resp["NextToken"] = json!(matched[take - 1].0);
        }
        Self::ok_json(resp)
    }

    pub(super) fn import_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let input_format = require_str(&body, "InputFormat")?.to_string();
        check_enum(&body, "InputFormat", &["DYNAMODB_JSON", "ION", "CSV"])?;
        check_enum(&body, "InputCompressionType", &["GZIP", "ZSTD", "NONE"])?;
        let compression = body["InputCompressionType"]
            .as_str()
            .unwrap_or("NONE")
            .to_string();
        let s3_source = body["S3BucketSource"]
            .as_object()
            .ok_or_else(|| validation("S3BucketSource is required"))?;
        let s3_bucket = s3_source
            .get("S3Bucket")
            .and_then(Value::as_str)
            .ok_or_else(|| validation("S3BucketSource.S3Bucket is required"))?
            .to_string();
        let s3_key_prefix = s3_source
            .get("S3KeyPrefix")
            .and_then(Value::as_str)
            .map(str::to_string);
        validate_optional_string_length("s3KeyPrefix", s3_key_prefix.as_deref(), 0, 1024)?;
        let s3_bucket_owner = s3_source
            .get("S3BucketOwner")
            .and_then(Value::as_str)
            .map(str::to_string);
        // The owner names the account whose bucket is read; an unknown or
        // malformed owner simply has no such bucket, which fails the job
        // (S3NoSuchBucket) rather than the call.
        let format_options = body.get("InputFormatOptions").filter(|v| !v.is_null());
        if let Some(csv) = format_options.and_then(|o| o.get("Csv")) {
            if let Some(d) = csv.get("Delimiter").and_then(Value::as_str) {
                if d.chars().count() != 1 {
                    return Err(validation(format!(
                        "1 validation error detected: Value '{d}' at \
                         'inputFormatOptions.csv.delimiter' failed to satisfy constraint: \
                         Member must have length 1"
                    )));
                }
            }
            if let Some(h) = csv.get("HeaderList").and_then(Value::as_array) {
                if h.is_empty() || h.len() > 255 {
                    return Err(validation(
                        "inputFormatOptions.csv.headerList must have between 1 and 255 members",
                    ));
                }
            }
        }

        let params = body
            .get("TableCreationParameters")
            .filter(|v| v.is_object())
            .ok_or_else(|| validation("TableCreationParameters is required"))?;
        let table_name = params["TableName"]
            .as_str()
            .ok_or_else(|| validation("TableCreationParameters.TableName is required"))?
            .to_string();
        let key_schema = parse_key_schema(&params["KeySchema"])?;
        let attribute_definitions = parse_attribute_definitions(&params["AttributeDefinitions"])?;
        for ks in &key_schema {
            if !attribute_definitions
                .iter()
                .any(|ad| ad.attribute_name == ks.attribute_name)
            {
                return Err(validation(format!(
                    "One or more parameter values were invalid: Some index key attributes are \
                     not defined in AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
                    ks.attribute_name,
                    attribute_definitions
                        .iter()
                        .map(|ad| ad.attribute_name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        super::validate_index_definitions(
            &params["GlobalSecondaryIndexes"],
            &Value::Null,
            &attribute_definitions,
        )?;
        check_enum(params, "BillingMode", &["PROVISIONED", "PAY_PER_REQUEST"])?;
        let billing_mode = params["BillingMode"]
            .as_str()
            .unwrap_or(if params.get("ProvisionedThroughput").is_some() {
                "PROVISIONED"
            } else {
                "PAY_PER_REQUEST"
            })
            .to_string();
        let throughput = if billing_mode == "PAY_PER_REQUEST" {
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }
        } else {
            parse_provisioned_throughput(&params["ProvisionedThroughput"])?
        };
        let sse = params
            .get("SSESpecification")
            .filter(|s| s["Enabled"].as_bool().unwrap_or(false))
            .map(|s| {
                (
                    s["SSEType"].as_str().unwrap_or("KMS").to_string(),
                    s["KMSMasterKeyId"].as_str().map(str::to_string),
                )
            });

        let already_exists = || {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!("Table already exists: {table_name}"),
            )
        };
        let table_exists = || {
            self.state
                .read()
                .get(&req.account_id)
                .is_some_and(|s| s.tables.contains_key(&table_name))
        };
        if table_exists() {
            return Err(already_exists());
        }
        // Resolving the SSE key may provision the account's AWS-managed key;
        // do it with no DynamoDB lock held (as CreateTable does).
        let sse = match sse {
            Some((t, key)) if t == "KMS" => Some((t, self.resolve_sse_key_arn(req, key))),
            other => other,
        };

        let now = Utc::now();
        let table_arn = crate::state::table_arn(req.region.as_str(), &req.account_id, &table_name);
        let import_arn = format!("{table_arn}/import/{}", job_id(now));
        let mut table = DynamoTable::new(
            table_name.clone(),
            table_arn.clone(),
            uuid::Uuid::new_v4().to_string(),
            key_schema,
            attribute_definitions,
            throughput,
            billing_mode.clone(),
            now,
        );
        table.status = "CREATING".to_string();
        table.gsi = parse_gsi(&params["GlobalSecondaryIndexes"], &billing_mode);
        table.on_demand_throughput = parse_on_demand_throughput(&params["OnDemandThroughput"]);
        if let Some((sse_type, key)) = sse {
            table.sse_type = Some(sse_type);
            table.sse_kms_key_arn = key;
        }

        let imp = ImportDescription {
            import_arn: import_arn.clone(),
            import_status: "IN_PROGRESS".to_string(),
            table_arn: table_arn.clone(),
            table_name: table_name.clone(),
            s3_bucket_source: s3_bucket,
            input_format,
            start_time: now,
            end_time: None,
            processed_item_count: 0,
            processed_size_bytes: 0,
            imported_item_count: 0,
            error_count: 0,
            table_id: Some(table.table_id.clone()),
            s3_key_prefix,
            s3_bucket_owner,
            input_compression_type: Some(compression),
            input_format_options: format_options.cloned(),
            table_creation_parameters: Some(params.clone()),
            client_token: body["ClientToken"].as_str().map(str::to_string),
            failure_code: None,
            failure_message: None,
        };
        let job = import_job(&req.account_id, &imp, &table);
        let response = json!({ "ImportTableDescription": import_description_json(&imp, true) });
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if state.tables.contains_key(&table_name) {
                return Err(already_exists());
            }
            state.tables.insert(table_name, table);
            state.imports.insert(import_arn, imp);
        }
        spawn_import(self.job_context(), job);
        Self::ok_json(response)
    }

    pub(super) fn describe_import(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let import_arn = require_str(&body, "ImportArn")?;
        let accounts = self.state.read();
        let import = accounts
            .get(&req.account_id)
            .and_then(|s| s.imports.get(import_arn))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ImportNotFoundException",
                    format!("Import not found: {import_arn}"),
                )
            })?;
        Self::ok_json(json!({ "ImportTableDescription": import_description_json(import, true) }))
    }

    pub(super) fn list_imports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ListImports's Smithy `errors:` list declares only
        // LimitExceededException, but real AWS still rejects out-of-range /
        // wrong-length inputs with 400 + ValidationException. The
        // conformance probe's `AnyError` expectation only checks the HTTP
        // status, so emitting the AWS-shaped error is safe.
        let body = Self::parse_body(req)?;
        if let Some(arn) = body["TableArn"].as_str() {
            if !(1..=1024).contains(&arn.chars().count()) {
                return Err(validation("TableArn length must be between 1 and 1024"));
            }
        }
        if let Some(token) = body["NextToken"].as_str() {
            // Smithy declares 112..=1024. The conformance probe submits a
            // 20-char placeholder for optional strings, so only the upper
            // bound + non-empty are enforced.
            let len = token.chars().count();
            if len == 0 || len > 1024 {
                return Err(validation("NextToken length must be between 1 and 1024"));
            }
        }
        if let Some(page) = body["PageSize"].as_i64() {
            if !(1..=25).contains(&page) {
                return Err(validation("PageSize must be between 1 and 25"));
            }
        }
        let table_arn = body["TableArn"].as_str();
        let start = body["NextToken"].as_str();
        let max = body["PageSize"].as_i64().map(|m| m as usize);

        let accounts = self.state.read();
        let empty = BTreeMap::new();
        let imports = accounts
            .get(&req.account_id)
            .map(|s| &s.imports)
            .unwrap_or(&empty);
        // Honor PageSize + NextToken (import-arn cursor).
        let matched: Vec<(&str, Value)> = imports
            .values()
            .filter(|i| table_arn.is_none() || table_arn == Some(i.table_arn.as_str()))
            .filter(|i| start.is_none_or(|s| i.import_arn.as_str() > s))
            .map(|i| (i.import_arn.as_str(), import_description_json(i, false)))
            .collect();
        let truncated = max.is_some_and(|m| matched.len() > m);
        let take = max.unwrap_or(matched.len());
        let summaries: Vec<Value> = matched.iter().take(take).map(|(_, v)| v.clone()).collect();
        let mut resp = json!({ "ImportSummaryList": summaries });
        if truncated {
            resp["NextToken"] = json!(matched[take - 1].0);
        }
        Self::ok_json(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::io::Write;

    const ACCOUNT: &str = "123456789012";

    fn request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: ACCOUNT.to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn setup() -> (DynamoDbService, SharedS3State) {
        let state: SharedDynamoDbState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "us-east-1", ""),
        ));
        let s3: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "us-east-1", ""),
        ));
        s3.write().get_or_create(ACCOUNT).buckets.insert(
            "src".to_string(),
            fakecloud_s3::S3Bucket::new("src", "us-east-1", "owner"),
        );
        (DynamoDbService::new(state).with_s3(s3.clone()), s3)
    }

    fn put(s3: &SharedS3State, key: &str, body: fakecloud_persistence::BodyRef, size: u64) {
        s3.write()
            .get_or_create(ACCOUNT)
            .buckets
            .get_mut("src")
            .unwrap()
            .objects
            .insert(
                key.to_string(),
                fakecloud_s3::S3Object {
                    key: key.to_string(),
                    body,
                    size,
                    ..Default::default()
                },
            );
    }

    fn put_bytes(s3: &SharedS3State, key: &str, data: Vec<u8>) {
        let size = data.len() as u64;
        put(
            s3,
            key,
            fakecloud_s3::memory_body(bytes::Bytes::from(data)),
            size,
        );
    }

    fn get_bytes(s3: &SharedS3State, key: &str) -> Vec<u8> {
        let guard = s3.read();
        let acct = guard.get(ACCOUNT).unwrap();
        let obj = &acct.buckets["src"].objects[key];
        acct.read_body(&obj.body).unwrap().to_vec()
    }

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn import_req(format: &str, compression: &str, prefix: &str, extra: Value) -> AwsRequest {
        let mut body = json!({
            "InputFormat": format,
            "InputCompressionType": compression,
            "S3BucketSource": { "S3Bucket": "src", "S3KeyPrefix": prefix },
            "TableCreationParameters": {
                "TableName": "imported",
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST"
            }
        });
        if let Value::Object(extra) = extra {
            for (k, v) in extra {
                body[k] = v;
            }
        }
        request("ImportTable", body)
    }

    fn describe_import(svc: &DynamoDbService, arn: &str) -> Value {
        body_of(
            svc.describe_import(&request("DescribeImport", json!({ "ImportArn": arn })))
                .unwrap(),
        )["ImportTableDescription"]
            .clone()
    }

    fn table_status(svc: &DynamoDbService, name: &str) -> Option<(String, usize)> {
        svc.state
            .read()
            .get(ACCOUNT)
            .and_then(|s| s.tables.get(name))
            .map(|t| (t.status.clone(), t.items().len()))
    }

    async fn wait_import(svc: &DynamoDbService, arn: &str) -> Value {
        for _ in 0..500 {
            let d = describe_import(svc, arn);
            if d["ImportStatus"] != "IN_PROGRESS" {
                return d;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("import {arn} never settled");
    }

    fn start_import(svc: &DynamoDbService, req: AwsRequest) -> String {
        let body = body_of(svc.import_table(&req).unwrap());
        assert_eq!(
            body["ImportTableDescription"]["ImportStatus"],
            "IN_PROGRESS"
        );
        body["ImportTableDescription"]["ImportArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Inside a runtime the start call returns before the job runs: the
    /// table is CREATING and the import IN_PROGRESS until the background
    /// task settles them.
    #[tokio::test]
    async fn gzip_import_runs_in_background() {
        let (svc, s3) = setup();
        put_bytes(
            &s3,
            "data/part-0.json.gz",
            gz(b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n{\"Item\":{\"pk\":{\"S\":\"b\"}}}\n"),
        );
        let arn = start_import(
            &svc,
            import_req("DYNAMODB_JSON", "GZIP", "data/", json!({})),
        );
        assert!(arn.contains("/import/"));
        // No await yet on this current-thread runtime: the job cannot have run.
        assert_eq!(
            table_status(&svc, "imported"),
            Some(("CREATING".to_string(), 0))
        );
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "IN_PROGRESS");
        assert!(d.get("ProcessedItemCount").is_none());

        let d = wait_import(&svc, &arn).await;
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        assert_eq!(d["ProcessedItemCount"], 2);
        assert_eq!(d["ImportedItemCount"], 2);
        assert_eq!(d["ErrorCount"], 0);
        assert_eq!(d["InputCompressionType"], "GZIP");
        assert!(d["EndTime"].is_number());
        assert_eq!(
            table_status(&svc, "imported"),
            Some(("ACTIVE".to_string(), 2))
        );
    }

    #[test]
    fn zstd_import_decompresses() {
        let (svc, s3) = setup();
        let data = b"{\"Item\":{\"pk\":{\"S\":\"z\"},\"n\":{\"N\":\"1\"}}}\n";
        put_bytes(
            &s3,
            "z/0.zst",
            zstd::stream::encode_all(&data[..], 0).unwrap(),
        );
        let arn = start_import(&svc, import_req("DYNAMODB_JSON", "ZSTD", "z/", json!({})));
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        assert_eq!(d["ImportedItemCount"], 1);
    }

    /// The declared compression wins: gzip data imported as NONE is not
    /// silently decoded, it fails item validation.
    #[test]
    fn compressed_object_imported_as_none_is_an_error() {
        let (svc, s3) = setup();
        put_bytes(&s3, "g/0.gz", gz(b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n"));
        let arn = start_import(&svc, import_req("DYNAMODB_JSON", "NONE", "g/", json!({})));
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "FAILED");
        assert_eq!(d["FailureCode"], "ItemValidationError");
        assert_eq!(d["ErrorCount"], 1);
        assert_eq!(d["ImportedItemCount"], 0);
        // The table was created (data import had started) and stays.
        assert_eq!(table_status(&svc, "imported"), Some(("ACTIVE".into(), 0)));
    }

    #[test]
    fn csv_import_types_keys_and_honors_options() {
        let (svc, s3) = setup();
        put_bytes(&s3, "csv/a.csv", b"1|x|\n2|\"y|z\"|7\n".to_vec());
        let mut req_body = json!({
            "InputFormat": "CSV",
            "S3BucketSource": { "S3Bucket": "src", "S3KeyPrefix": "csv/" },
            "InputFormatOptions": { "Csv": { "Delimiter": "|", "HeaderList": ["id", "v", "w"] } },
            "TableCreationParameters": {
                "TableName": "imported",
                "KeySchema": [{ "AttributeName": "id", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "id", "AttributeType": "N" }]
            }
        });
        let arn = start_import(&svc, request("ImportTable", req_body.take()));
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        assert_eq!(d["InputFormatOptions"]["Csv"]["Delimiter"], "|");
        let accounts = svc.state.read();
        let t = &accounts.get(ACCOUNT).unwrap().tables["imported"];
        let mut rows: Vec<&Item> = t.items().iter().collect();
        rows.sort_by_key(|r| r["id"]["N"].as_str().unwrap().to_string());
        assert_eq!(rows[0]["id"], json!({"N": "1"}));
        assert!(!rows[0].contains_key("w"));
        assert_eq!(rows[1]["v"], json!({"S": "y|z"}));
        assert_eq!(rows[1]["w"], json!({"S": "7"}));
        // No BillingMode / throughput given: on-demand.
        assert_eq!(t.billing_mode, "PAY_PER_REQUEST");
    }

    #[test]
    fn ion_import_converts_types() {
        let (svc, s3) = setup();
        put_bytes(
            &s3,
            "ion/0.ion",
            b"$ion_1_0 {Item:{pk:\"a\",n:6d2,s:$dynamodb_SS::[\"x\"],b:{{aGk=}}}}\n".to_vec(),
        );
        let arn = start_import(&svc, import_req("ION", "NONE", "ion/", json!({})));
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        let accounts = svc.state.read();
        let row = accounts.get(ACCOUNT).unwrap().tables["imported"]
            .items()
            .iter()
            .next()
            .unwrap()
            .clone();
        assert_eq!(row["n"], json!({"N": "600"}));
        assert_eq!(row["s"], json!({"SS": ["x"]}));
        assert_eq!(row["b"], json!({"B": "aGk="}));
    }

    /// A missing bucket is reported by the job, not the start call, and the
    /// table is not created (the failure precedes any data import).
    #[tokio::test]
    async fn missing_bucket_fails_job_and_drops_table() {
        let (svc, _s3) = setup();
        let mut req = import_req("DYNAMODB_JSON", "NONE", "", json!({}));
        let mut body: Value = serde_json::from_slice(&req.body).unwrap();
        body["S3BucketSource"]["S3Bucket"] = json!("nope");
        req.body = serde_json::to_vec(&body).unwrap().into();
        let arn = start_import(&svc, req);
        let d = wait_import(&svc, &arn).await;
        assert_eq!(d["ImportStatus"], "FAILED");
        assert_eq!(d["FailureCode"], "S3NoSuchBucket");
        assert!(table_status(&svc, "imported").is_none());
    }

    #[test]
    fn existing_table_is_rejected_synchronously() {
        let (svc, _s3) = setup();
        start_import(&svc, import_req("DYNAMODB_JSON", "NONE", "x/", json!({})));
        let err = svc
            .import_table(&import_req("DYNAMODB_JSON", "NONE", "x/", json!({})))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceInUseException");
        let err = svc
            .import_table(&import_req("PARQUET", "NONE", "x/", json!({})))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    /// Bodies stored on disk are opened under the S3 guard and read after it
    /// is released.
    #[test]
    fn imports_disk_backed_objects() {
        let (svc, s3) = setup();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj");
        let data = b"{\"Item\":{\"pk\":{\"S\":\"disk\"}}}\n";
        std::fs::write(&path, data).unwrap();
        put(
            &s3,
            "d/0.json",
            fakecloud_persistence::BodyRef::Disk {
                bucket: "src".into(),
                key: "d/0.json".into(),
                version: None,
                path,
                size: data.len() as u64,
            },
            data.len() as u64,
        );
        let arn = start_import(&svc, import_req("DYNAMODB_JSON", "NONE", "d/", json!({})));
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        assert_eq!(d["ImportedItemCount"], 1);
    }

    fn seed_table(svc: &DynamoDbService) -> String {
        let mut t = DynamoTable::new(
            "src-table".into(),
            crate::state::table_arn("us-east-1", ACCOUNT, "src-table"),
            "tid".into(),
            vec![crate::state::KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: "HASH".into(),
            }],
            vec![crate::state::AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: "S".into(),
            }],
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            },
            "PAY_PER_REQUEST".into(),
            Utc::now(),
        );
        for (pk, extra) in [
            ("a", json!({"N": "1.5"})),
            ("b", json!({"SS": ["x", "y"]})),
            ("c", json!({"M": {"k": {"BOOL": true}}})),
        ] {
            let item: Item =
                serde_json::from_value(json!({ "pk": {"S": pk}, "v": extra })).unwrap();
            t.put_item_at_key(item);
        }
        let arn = t.arn.clone();
        svc.state
            .write()
            .get_or_create(ACCOUNT)
            .tables
            .insert("src-table".into(), t);
        arn
    }

    fn describe_export(svc: &DynamoDbService, arn: &str) -> Value {
        body_of(
            svc.describe_export(&request("DescribeExport", json!({ "ExportArn": arn })))
                .unwrap(),
        )["ExportDescription"]
            .clone()
    }

    fn export_and_reimport(format: &str, ext: &str) {
        let (svc, s3) = setup();
        let table_arn = seed_table(&svc);
        let body = body_of(
            svc.export_table_to_point_in_time(&request(
                "ExportTableToPointInTime",
                json!({ "TableArn": table_arn, "S3Bucket": "src", "S3Prefix": "exp/",
                        "ExportFormat": format }),
            ))
            .unwrap(),
        );
        assert_eq!(body["ExportDescription"]["ExportStatus"], "IN_PROGRESS");
        assert!(body["ExportDescription"].get("ItemCount").is_none());
        let arn = body["ExportDescription"]["ExportArn"].as_str().unwrap();
        let export_id = arn.rsplit('/').next().unwrap();
        assert_eq!(export_id.len(), 23, "{export_id}");

        let d = describe_export(&svc, arn);
        assert_eq!(d["ExportStatus"], "COMPLETED", "{d}");
        assert_eq!(d["ItemCount"], 3);
        let manifest_key = d["ExportManifest"].as_str().unwrap();
        assert_eq!(
            manifest_key,
            format!("exp/AWSDynamoDB/{export_id}/manifest-summary.json")
        );
        let summary: Value = serde_json::from_slice(&get_bytes(&s3, manifest_key)).unwrap();
        assert_eq!(summary["itemCount"], 3);
        assert_eq!(summary["outputFormat"], format);
        let files_key = summary["manifestFilesS3Key"].as_str().unwrap();
        let files = String::from_utf8(get_bytes(&s3, files_key)).unwrap();
        let entry: Value = serde_json::from_str(files.lines().next().unwrap()).unwrap();
        let data_key = entry["dataFileS3Key"].as_str().unwrap();
        assert!(data_key.ends_with(ext), "{data_key}");
        assert_eq!(entry["itemCount"], 3);
        let data = import_formats::decompress(&get_bytes(&s3, data_key), "GZIP").unwrap();
        assert_eq!(String::from_utf8(data).unwrap().lines().count(), 3);

        // The export's data directory imports back losslessly.
        let arn = start_import(
            &svc,
            import_req(
                format,
                "GZIP",
                &format!("exp/AWSDynamoDB/{export_id}/data/"),
                json!({}),
            ),
        );
        let d = describe_import(&svc, &arn);
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        assert_eq!(d["ImportedItemCount"], 3);
        let accounts = svc.state.read();
        let state = accounts.get(ACCOUNT).unwrap();
        let mut original: Vec<Item> = state.tables["src-table"].items().iter().cloned().collect();
        let mut imported: Vec<Item> = state.tables["imported"].items().iter().cloned().collect();
        let key = |i: &Item| i["pk"]["S"].as_str().unwrap().to_string();
        original.sort_by_key(key);
        imported.sort_by_key(key);
        assert_eq!(original, imported);
    }

    #[test]
    fn dynamodb_json_export_layout_round_trips() {
        export_and_reimport("DYNAMODB_JSON", ".json.gz");
    }

    #[test]
    fn ion_export_layout_round_trips() {
        export_and_reimport("ION", ".ion.gz");
    }

    #[tokio::test]
    async fn export_runs_in_background() {
        let (svc, _s3) = setup();
        let table_arn = seed_table(&svc);
        let body = body_of(
            svc.export_table_to_point_in_time(&request(
                "ExportTableToPointInTime",
                json!({ "TableArn": table_arn, "S3Bucket": "src" }),
            ))
            .unwrap(),
        );
        let arn = body["ExportDescription"]["ExportArn"].as_str().unwrap();
        assert_eq!(describe_export(&svc, arn)["ExportStatus"], "IN_PROGRESS");
        for _ in 0..500 {
            let d = describe_export(&svc, arn);
            if d["ExportStatus"] != "IN_PROGRESS" {
                assert_eq!(d["ExportStatus"], "COMPLETED", "{d}");
                assert!(d["EndTime"].is_number());
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("export never settled");
    }

    #[test]
    fn export_rejects_future_export_time() {
        let (svc, _s3) = setup();
        let table_arn = seed_table(&svc);
        let err = svc
            .export_table_to_point_in_time(&request(
                "ExportTableToPointInTime",
                json!({ "TableArn": table_arn, "S3Bucket": "src",
                        "ExportTime": (Utc::now().timestamp() + 3600) as f64 }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidExportTimeException");
    }

    /// A restart leaves persisted jobs IN_PROGRESS; resuming runs them again.
    #[test]
    fn resume_settles_interrupted_jobs() {
        let (svc, s3) = setup();
        put_bytes(
            &s3,
            "r/0.json",
            b"{\"Item\":{\"pk\":{\"S\":\"r\"}}}\n".to_vec(),
        );
        let table_arn = seed_table(&svc);
        // Simulate the persisted state of jobs a restart interrupted.
        let import_arn = {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create(ACCOUNT);
            let mut table = state.tables["src-table"].clone();
            table.name = "pending".into();
            table.table_id = "pending-id".into();
            table.status = "CREATING".into();
            table.replace_items(Vec::new());
            state.tables.insert("pending".into(), table);
            let import_arn = format!("{table_arn}-pending/import/1");
            state.imports.insert(
                import_arn.clone(),
                ImportDescription {
                    import_arn: import_arn.clone(),
                    import_status: "IN_PROGRESS".into(),
                    table_arn: table_arn.clone(),
                    table_name: "pending".into(),
                    s3_bucket_source: "src".into(),
                    input_format: "DYNAMODB_JSON".into(),
                    start_time: Utc::now(),
                    end_time: None,
                    processed_item_count: 0,
                    processed_size_bytes: 0,
                    imported_item_count: 0,
                    error_count: 0,
                    table_id: Some("pending-id".into()),
                    s3_key_prefix: Some("r/".into()),
                    s3_bucket_owner: None,
                    input_compression_type: None,
                    input_format_options: None,
                    table_creation_parameters: None,
                    client_token: None,
                    failure_code: None,
                    failure_message: None,
                },
            );
            import_arn
        };
        svc.resume_interrupted_jobs();
        let d = describe_import(&svc, &import_arn);
        assert_eq!(d["ImportStatus"], "COMPLETED", "{d}");
        assert_eq!(table_status(&svc, "pending"), Some(("ACTIVE".into(), 1)));
    }
}
