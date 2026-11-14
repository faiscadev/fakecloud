//! DynamoDB bulk operations: `StartBulkPutItems` reads items from S3 and
//! writes them to an existing table; `DescribeBulkOperation`,
//! `ListBulkOperations` and `CancelBulkOperation` follow it.
//!
//! The start call validates the request, records the operation
//! `IN_PROGRESS` and returns; the S3 reads, decompression, parsing and writes
//! run on a background job (the same machinery as `ImportTable`). The job
//! commits one input object at a time: each record is written the way a
//! `PutItem` would write it (overwriting an item with the same key, recorded
//! on the table's stream and replicated to its global table replicas), and a
//! record `PutItem` would reject is counted as failed and, when the output
//! asks for `ERROR`, reported to the output bucket. A cancellation stops the
//! job before its next object; items already written stay.

use std::collections::{BTreeMap, HashMap};

use chrono::{Duration, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{BulkOperation, BulkOperationUsage, DynamoTable, Projection};

use super::import_export::{
    bucket_exists, check_enum, epoch_secs, export_encryption, job_id, open_source_objects,
    put_s3_object, validation, JobContext, SourceError, NO_SUCH_BUCKET_MESSAGE,
};
use super::import_formats::{self, ParsedRow};
use super::{
    extract_key, item_size, normalize_item_numbers, require_str, validate_index_keys_in_item,
    validate_item_attribute_values, validate_key_in_item, DynamoDbService, MAX_ITEM_SIZE,
};

type Item = HashMap<String, Value>;

/// `ListBulkOperations` page size when `MaxResults` is omitted.
const DEFAULT_PAGE_SIZE: usize = 10;
/// How long a client token identifies the operation it started.
const IDEMPOTENCY_WINDOW_HOURS: i64 = 8;
/// `ListBulkOperations` reports operations started within this window.
const LIST_WINDOW_DAYS: i64 = 90;

const CANCELLED_CODE: &str = "BulkOperationCancelled";
const CANCELLED_MESSAGE: &str = "The bulk operation was cancelled by request.";

fn not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Requested resource not found: Bulk operation: {arn} not found"),
    )
}

fn constraint(value: &str, member: &str, rule: &str) -> AwsServiceError {
    validation(format!(
        "1 validation error detected: Value '{value}' at '{member}' failed to satisfy \
         constraint: {rule}"
    ))
}

fn check_length(
    v: Option<&str>,
    member: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    match v {
        Some(s) if !(min..=max).contains(&s.chars().count()) => Err(constraint(
            s,
            member,
            &format!("Member must have length between {min} and {max}"),
        )),
        _ => Ok(()),
    }
}

fn check_account_id(v: Option<&str>, member: &str) -> Result<(), AwsServiceError> {
    match v {
        Some(s) if s.len() != 12 || !s.bytes().all(|b| b.is_ascii_digit()) => Err(constraint(
            s,
            member,
            "Member must satisfy regular expression pattern: [0-9]{12}",
        )),
        _ => Ok(()),
    }
}

/// The request's `InputS3Configuration`, checked, with the compression it
/// defaults to.
fn input_configuration(body: &Value) -> Result<Value, AwsServiceError> {
    let input = body
        .get("InputS3Configuration")
        .filter(|v| v.is_object())
        .ok_or_else(|| {
            validation(
                "1 validation error detected: Value null at 'inputS3Configuration' failed to \
                 satisfy constraint: Member must not be null",
            )
        })?;
    let bucket = input["S3Bucket"].as_str().ok_or_else(|| {
        validation(
            "1 validation error detected: Value null at 'inputS3Configuration.s3Bucket' failed \
             to satisfy constraint: Member must not be null",
        )
    })?;
    check_length(Some(bucket), "inputS3Configuration.s3Bucket", 0, 255)?;
    check_length(
        input["S3KeyPrefix"].as_str(),
        "inputS3Configuration.s3KeyPrefix",
        0,
        1024,
    )?;
    check_account_id(
        input["S3BucketOwner"].as_str(),
        "inputS3Configuration.s3BucketOwner",
    )?;
    if input["InputFormat"].as_str().is_none() {
        return Err(validation(
            "1 validation error detected: Value null at 'inputS3Configuration.inputFormat' \
             failed to satisfy constraint: Member must not be null",
        ));
    }
    check_enum(input, "InputFormat", &["DYNAMODB_JSON", "ION"])?;
    check_enum(input, "InputCompressionType", &["GZIP", "ZSTD", "NONE"])?;
    let mut input = input.clone();
    if input["InputCompressionType"].as_str().is_none() {
        input["InputCompressionType"] = json!("NONE");
    }
    Ok(input)
}

/// The request's `OutputS3Configuration`, checked.
fn output_configuration(body: &Value) -> Result<Value, AwsServiceError> {
    let output = body
        .get("OutputS3Configuration")
        .filter(|v| v.is_object())
        .ok_or_else(|| {
            validation(
                "1 validation error detected: Value null at 'outputS3Configuration' failed to \
                 satisfy constraint: Member must not be null",
            )
        })?;
    let bucket = output["S3Bucket"]
        .as_str()
        .ok_or_else(|| validation("OutputS3Configuration.S3Bucket is required"))?;
    check_length(Some(bucket), "outputS3Configuration.s3Bucket", 0, 255)?;
    check_length(
        output["S3KeyPrefix"].as_str(),
        "outputS3Configuration.s3KeyPrefix",
        0,
        1024,
    )?;
    check_account_id(
        output["S3BucketOwner"].as_str(),
        "outputS3Configuration.s3BucketOwner",
    )?;
    check_enum(output, "S3SseAlgorithm", &["AES256", "KMS"])?;
    let kms_key = output["S3SseKmsKeyId"].as_str();
    check_length(kms_key, "outputS3Configuration.s3SseKmsKeyId", 1, 2048)?;
    if kms_key.is_some() && output["S3SseAlgorithm"].as_str() != Some("KMS") {
        return Err(validation(
            "OutputS3Configuration.S3SseKmsKeyId can only be specified when S3SseAlgorithm is KMS",
        ));
    }
    let types = output["OutputType"].as_array().ok_or_else(|| {
        validation(
            "1 validation error detected: Value null at 'outputS3Configuration.outputType' \
             failed to satisfy constraint: Member must not be null",
        )
    })?;
    if types.is_empty() {
        return Err(validation(
            "OutputS3Configuration.OutputType must include ERROR, CONDITION_CHECK_FAILURE or both",
        ));
    }
    for t in types {
        match t.as_str() {
            Some("ERROR" | "CONDITION_CHECK_FAILURE") => {}
            other => {
                return Err(constraint(
                    other.unwrap_or("null"),
                    "outputS3Configuration.outputType",
                    "Member must satisfy enum value set: [ERROR, CONDITION_CHECK_FAILURE]",
                ));
            }
        }
    }
    Ok(output.clone())
}

fn page_token(offset: usize) -> String {
    format!("{offset:016x}")
}

fn parse_page_token(token: &str) -> Result<usize, AwsServiceError> {
    let well_formed = !token.is_empty()
        && token.len() <= 1024
        && token.len().is_multiple_of(16)
        && token
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    let offset = token
        .get(token.len().saturating_sub(16)..)
        .filter(|_| well_formed)
        .and_then(|t| usize::from_str_radix(t, 16).ok());
    offset.ok_or_else(|| {
        constraint(
            token,
            "nextToken",
            "Member must satisfy regular expression pattern: ([0-9a-f]{16})+",
        )
    })
}

fn usage_json(op: &BulkOperation) -> Value {
    let u = &op.usage;
    let index_total: i64 = u.global_secondary_indexes.values().sum::<i64>()
        + u.local_secondary_indexes.values().sum::<i64>();
    let mut usage = json!({
        "TableName": op.table_name,
        "BulkPutBytes": u.table_bytes + index_total,
        "Table": { "BulkPutBytes": u.table_bytes },
    });
    let map = |m: &BTreeMap<String, i64>| {
        Value::Object(m.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
    };
    if !u.local_secondary_indexes.is_empty() {
        usage["LocalSecondaryIndexes"] = map(&u.local_secondary_indexes);
    }
    if !u.global_secondary_indexes.is_empty() {
        usage["GlobalSecondaryIndexes"] = map(&u.global_secondary_indexes);
    }
    if !u.vector_indexes.is_empty() {
        usage["VectorIndexes"] = map(&u.vector_indexes);
    }
    usage
}

pub(super) fn description_json(op: &BulkOperation) -> Value {
    let mut d = json!({
        "BulkOperationArn": op.arn,
        "BulkOperationStatus": op.status,
        "TableDetails": [{ "TableName": op.table_name, "TableId": op.table_id }],
        "InputS3Configuration": op.input_s3_configuration,
        "OutputS3Configuration": op.output_s3_configuration,
        "StartTime": epoch_secs(op.start_time),
        "ProcessedItemCount": op.processed_item_count,
        "ProcessedInputBytes": op.processed_input_bytes,
        "FailedItemCount": op.failed_item_count,
        "BulkOperationUsage": [usage_json(op)],
        "PerformanceTier": op.performance_tier.as_deref().unwrap_or("STANDARD"),
    });
    if let Some(t) = &op.client_token {
        d["ClientToken"] = json!(t);
    }
    if let Some(end) = op.end_time {
        d["EndTime"] = json!(epoch_secs(end));
    }
    if let Some(total) = op.total_input_bytes {
        d["TotalInputBytes"] = json!(total);
    }
    if let Some(units) = op.max_write_request_units {
        d["MaxWriteRequestUnits"] = json!(units);
    }
    if matches!(op.status.as_str(), "FAILED" | "CANCELLED") {
        if let Some(c) = &op.failure_code {
            d["FailureCode"] = json!(c);
        }
        if let Some(m) = &op.failure_message {
            d["FailureMessage"] = json!(m);
        }
    }
    d
}

// ---------------------------------------------------------------------------
// Background job
// ---------------------------------------------------------------------------

/// Everything the job needs, detached from the request.
struct BulkJob {
    account_id: String,
    region: String,
    arn: String,
    op_id: String,
    table_name: String,
    table_id: String,
    input_bucket: String,
    input_owner: String,
    input_prefix: String,
    input_format: String,
    compression: String,
    output_bucket: String,
    output_owner: String,
    output_prefix: String,
    report_errors: bool,
    sse_algorithm: Option<String>,
    sse_kms_key_id: Option<String>,
}

fn bulk_job(account_id: &str, region: &str, op: &BulkOperation) -> BulkJob {
    let input = &op.input_s3_configuration;
    let output = &op.output_s3_configuration;
    let s = |v: &Value| v.as_str().unwrap_or_default().to_string();
    BulkJob {
        account_id: account_id.to_string(),
        region: region.to_string(),
        arn: op.arn.clone(),
        op_id: op.arn.rsplit('/').next().unwrap_or_default().to_string(),
        table_name: op.table_name.clone(),
        table_id: op.table_id.clone(),
        input_bucket: s(&input["S3Bucket"]),
        input_owner: input["S3BucketOwner"]
            .as_str()
            .unwrap_or(account_id)
            .to_string(),
        input_prefix: s(&input["S3KeyPrefix"]),
        input_format: input["InputFormat"]
            .as_str()
            .unwrap_or("DYNAMODB_JSON")
            .to_string(),
        compression: input["InputCompressionType"]
            .as_str()
            .unwrap_or("NONE")
            .to_string(),
        output_bucket: s(&output["S3Bucket"]),
        output_owner: output["S3BucketOwner"]
            .as_str()
            .unwrap_or(account_id)
            .to_string(),
        output_prefix: s(&output["S3KeyPrefix"]),
        report_errors: output["OutputType"]
            .as_array()
            .is_some_and(|t| t.iter().any(|v| v.as_str() == Some("ERROR"))),
        sse_algorithm: output["S3SseAlgorithm"].as_str().map(str::to_string),
        sse_kms_key_id: output["S3SseKmsKeyId"].as_str().map(str::to_string),
    }
}

/// A record the job could not write, as reported to the output bucket.
struct RecordError {
    object_key: String,
    record_number: usize,
    message: String,
    item: Option<Item>,
}

fn settle(ctx: &JobContext, job: &BulkJob, f: impl FnOnce(&mut BulkOperation)) {
    ctx.with_account(&job.account_id, &job.region, |state| {
        if let Some(op) = state.bulk_operations.get_mut(&job.arn) {
            f(op);
        }
    });
}

fn finish(op: &mut BulkOperation, status: &str, failure: Option<(&str, &str)>) {
    op.status = status.to_string();
    op.end_time = Some(Utc::now());
    if let Some((code, message)) = failure {
        op.failure_code = Some(code.to_string());
        op.failure_message = Some(message.to_string());
    }
}

fn fail(ctx: &JobContext, job: &BulkJob, code: &str, message: &str) {
    settle(ctx, job, |op| finish(op, "FAILED", Some((code, message))));
}

/// Settle a cancelled operation; `true` when a cancellation was requested.
fn stop_if_cancelled(ctx: &JobContext, job: &BulkJob) -> bool {
    let mut cancelled = false;
    settle(ctx, job, |op| {
        if op.cancel_requested {
            cancelled = true;
            finish(op, "CANCELLED", Some((CANCELLED_CODE, CANCELLED_MESSAGE)));
        }
    });
    cancelled
}

fn parse_rows(job: &BulkJob, text: &str) -> Vec<ParsedRow> {
    match job.input_format.as_str() {
        "ION" => import_formats::parse_ion(text),
        _ => import_formats::parse_dynamodb_json(text),
    }
}

/// The part of `item` an index with `projection` and key attributes
/// `index_keys` stores, or `None` when the item is not in the index.
fn projected(
    table: &DynamoTable,
    item: &Item,
    index_keys: &[&str],
    projection: &Projection,
) -> Option<Item> {
    if index_keys.iter().any(|k| !item.contains_key(*k)) {
        return None;
    }
    if projection.projection_type == "ALL" {
        return Some(item.clone());
    }
    let mut out = extract_key(table, item);
    for k in index_keys {
        if let Some(v) = item.get(*k) {
            out.insert((*k).to_string(), v.clone());
        }
    }
    if projection.projection_type == "INCLUDE" {
        for name in &projection.non_key_attributes {
            if let Some(v) = item.get(name) {
                out.insert(name.clone(), v.clone());
            }
        }
    }
    Some(out)
}

/// Add the bytes writing `item` puts to the table and each of its indexes.
fn account_usage(usage: &mut BulkOperationUsage, table: &DynamoTable, item: &Item) {
    usage.table_bytes += item_size(item) as i64;
    for gsi in &table.gsi {
        let keys: Vec<&str> = gsi
            .key_schema
            .iter()
            .map(|k| k.attribute_name.as_str())
            .collect();
        if let Some(p) = projected(table, item, &keys, &gsi.projection) {
            *usage
                .global_secondary_indexes
                .entry(gsi.index_name.clone())
                .or_default() += item_size(&p) as i64;
        }
    }
    for lsi in &table.lsi {
        let keys: Vec<&str> = lsi
            .key_schema
            .iter()
            .map(|k| k.attribute_name.as_str())
            .collect();
        if let Some(p) = projected(table, item, &keys, &lsi.projection) {
            *usage
                .local_secondary_indexes
                .entry(lsi.index_name.clone())
                .or_default() += item_size(&p) as i64;
        }
    }
    for index in &table.vector_indexes {
        if let Some(v) = item.get(&index.vector_attribute) {
            let vector: Item = HashMap::from([(index.vector_attribute.clone(), v.clone())]);
            *usage
                .vector_indexes
                .entry(index.index_name.clone())
                .or_default() += item_size(&vector) as i64;
        }
    }
}

/// Why `PutItem` would refuse `item` on `table`, if it would.
fn rejection(table: &DynamoTable, item: &Item) -> Option<String> {
    let checks = [
        validate_key_in_item(table, item),
        validate_item_attribute_values(item),
        validate_index_keys_in_item(table, item),
        super::vectors::validate_vector_item(
            &table.vector_indexes,
            &table.attribute_definitions,
            item,
        ),
    ];
    if let Some(err) = checks.into_iter().find_map(Result::err) {
        return Some(err.message());
    }
    (item_size(item) > MAX_ITEM_SIZE)
        .then(|| "Item size has exceeded the maximum allowed size".to_string())
}

/// Write one object's parsed records to the table. `Err` when the table is
/// gone.
fn commit_rows(
    ctx: &JobContext,
    job: &BulkJob,
    object_key: &str,
    size: u64,
    rows: Vec<ParsedRow>,
    errors: &mut Vec<RecordError>,
) -> Result<(), ()> {
    let mut accounts = ctx.state.write();
    let written = {
        let state = accounts.regional_mut(&job.account_id, &job.region);
        let region = state.region.clone();
        let Some(table) = state
            .tables
            .get_mut(&job.table_name)
            .filter(|t| t.table_id == job.table_id)
        else {
            return Err(());
        };
        let mut usage = BulkOperationUsage::default();
        let mut written: Vec<Item> = Vec::new();
        let processed = rows.len() as i64;
        let mut failed = 0i64;
        for (i, row) in rows.into_iter().enumerate() {
            let mut item = match row {
                Ok(item) => item,
                Err(message) => {
                    failed += 1;
                    errors.push(RecordError {
                        object_key: object_key.to_string(),
                        record_number: i + 1,
                        message,
                        item: None,
                    });
                    continue;
                }
            };
            if let Some(message) = rejection(table, &item) {
                failed += 1;
                errors.push(RecordError {
                    object_key: object_key.to_string(),
                    record_number: i + 1,
                    message,
                    item: Some(item),
                });
                continue;
            }
            normalize_item_numbers(&mut item);
            let key = extract_key(table, &item);
            table.ensure_key_index();
            let old = table
                .find_item_index(&key)
                .map(|id| table.items()[id].clone());
            account_usage(&mut usage, table, &item);
            table.put_item_at_key(item.clone());
            if table.stream_enabled {
                let event = if old.is_some() { "MODIFY" } else { "INSERT" };
                if let Some(record) = crate::streams::generate_stream_record(
                    table,
                    event,
                    key.clone(),
                    old,
                    Some(item),
                    &region,
                ) {
                    crate::streams::add_stream_record(table, record);
                }
            }
            written.push(key);
        }
        if let Some(op) = state.bulk_operations.get_mut(&job.arn) {
            op.processed_item_count += processed;
            op.failed_item_count += failed;
            op.processed_input_bytes += size as i64;
            op.usage.table_bytes += usage.table_bytes;
            for (dst, src) in [
                (
                    &mut op.usage.global_secondary_indexes,
                    usage.global_secondary_indexes,
                ),
                (
                    &mut op.usage.local_secondary_indexes,
                    usage.local_secondary_indexes,
                ),
                (&mut op.usage.vector_indexes, usage.vector_indexes),
            ] {
                for (k, v) in src {
                    *dst.entry(k).or_default() += v;
                }
            }
        }
        written
    };
    super::replicas::replicate_keys(
        &mut accounts,
        &job.account_id,
        &job.region,
        &job.table_name,
        &written,
    );
    Ok(())
}

/// The key the job's error records are written under.
fn error_report_key(job: &BulkJob) -> String {
    let prefix = job.output_prefix.trim_end_matches('/');
    let base = format!(
        "AWSDynamoDB/BulkOperations/{}/errors/part-00000.json",
        job.op_id
    );
    if prefix.is_empty() {
        base
    } else {
        format!("{prefix}/{base}")
    }
}

fn error_report(errors: &[RecordError]) -> Vec<u8> {
    let mut out = String::new();
    for e in errors {
        let mut line = Map::new();
        line.insert("InputObjectKey".to_string(), json!(e.object_key));
        line.insert("RecordNumber".to_string(), json!(e.record_number));
        line.insert("ErrorCode".to_string(), json!("ValidationError"));
        line.insert("ErrorMessage".to_string(), json!(e.message));
        if let Some(item) = &e.item {
            line.insert("Item".to_string(), json!(item));
        }
        out.push_str(&Value::Object(line).to_string());
        out.push('\n');
    }
    out.into_bytes()
}

fn run_bulk(ctx: &JobContext, job: &BulkJob) {
    if stop_if_cancelled(ctx, job) {
        return;
    }
    if job.report_errors && !bucket_exists(ctx, &job.output_owner, &job.output_bucket) {
        return fail(ctx, job, "S3NoSuchBucket", NO_SUCH_BUCKET_MESSAGE);
    }
    let objects = match &ctx.s3_state {
        Some(s3) => open_source_objects(s3, &job.input_owner, &job.input_bucket, &job.input_prefix),
        None => Ok(Vec::new()),
    };
    let objects = match objects {
        Ok(o) => o,
        Err(SourceError::NoSuchBucket) => {
            return fail(ctx, job, "S3NoSuchBucket", NO_SUCH_BUCKET_MESSAGE);
        }
        Err(SourceError::Read(msg)) => return fail(ctx, job, "InternalServerError", &msg),
    };
    let total: u64 = objects.iter().map(|o| o.size).sum();
    settle(ctx, job, |op| op.total_input_bytes = Some(total as i64));

    let mut errors: Vec<RecordError> = Vec::new();
    for obj in objects {
        if stop_if_cancelled(ctx, job) {
            return;
        }
        // Read, decompress and parse with no lock held.
        let rows = match obj.body.read_all() {
            Err(e) if e.is_kms() => {
                return fail(ctx, job, "S3AccessDenied", &e.to_string());
            }
            Err(e) => vec![Err(format!("failed to read S3 object: {e}"))],
            Ok(raw) => match import_formats::decompress(&raw, &job.compression).and_then(|d| {
                String::from_utf8(d).map_err(|_| "S3 object is not valid UTF-8".to_string())
            }) {
                Ok(text) => parse_rows(job, &text),
                Err(e) => vec![Err(e)],
            },
        };
        if commit_rows(ctx, job, &obj.key, obj.size, rows, &mut errors).is_err() {
            return fail(
                ctx,
                job,
                "ResourceNotFoundException",
                &format!(
                    "Table {} was deleted while the bulk operation was in progress",
                    job.table_name
                ),
            );
        }
    }

    if job.report_errors && !errors.is_empty() {
        let written = put_s3_object(
            ctx,
            &job.output_owner,
            &job.output_bucket,
            &error_report_key(job),
            error_report(&errors),
            "application/json",
            export_encryption(job.sse_algorithm.as_deref(), job.sse_kms_key_id.as_deref()),
        );
        match written {
            Ok(()) => {}
            Err(SourceError::NoSuchBucket) => {
                return fail(ctx, job, "S3NoSuchBucket", NO_SUCH_BUCKET_MESSAGE);
            }
            Err(SourceError::Read(msg)) => return fail(ctx, job, "InternalServerError", &msg),
        }
    }
    let status = if errors.is_empty() {
        "COMPLETED"
    } else {
        "COMPLETED_WITH_ERRORS"
    };
    settle(ctx, job, |op| finish(op, status, None));
}

fn spawn_bulk(ctx: JobContext, job: BulkJob) {
    let job = std::sync::Arc::new(job);
    let on_panic = job.clone();
    ctx.run(
        move |ctx| run_bulk(ctx, &job),
        move |ctx| {
            fail(
                ctx,
                &on_panic,
                "InternalServerError",
                "The bulk operation failed unexpectedly",
            )
        },
    );
}

/// Resume bulk operations a restart interrupted: an `IN_PROGRESS` operation
/// starts over (rewriting an item it already wrote leaves it as it was), and
/// a `CANCELLING` one is cancelled.
pub(super) fn resume_interrupted_bulk_operations(ctx: &JobContext) {
    let mut jobs = Vec::new();
    {
        let mut accounts = ctx.state.write();
        for (account_id, region, state) in accounts.iter_regional_mut() {
            for op in state.bulk_operations.values_mut() {
                match op.status.as_str() {
                    "IN_PROGRESS" => {
                        op.processed_item_count = 0;
                        op.failed_item_count = 0;
                        op.processed_input_bytes = 0;
                        op.total_input_bytes = None;
                        op.usage = BulkOperationUsage::default();
                        jobs.push(bulk_job(account_id, region, op));
                    }
                    "CANCELLING" => {
                        finish(op, "CANCELLED", Some((CANCELLED_CODE, CANCELLED_MESSAGE)));
                    }
                    _ => {}
                }
            }
        }
    }
    for job in jobs {
        spawn_bulk(ctx.clone(), job);
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

impl DynamoDbService {
    pub(super) fn start_bulk_put_items(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        check_length(Some(table_name), "tableName", 1, 1024)?;
        let input = input_configuration(&body)?;
        let output = output_configuration(&body)?;
        let client_token = body["ClientToken"].as_str().map(str::to_string);
        if let Some(token) = client_token.as_deref() {
            if token.is_empty() || token.contains('$') {
                return Err(constraint(
                    token,
                    "clientToken",
                    "Member must satisfy regular expression pattern: [^\\$]+",
                ));
            }
        }
        let max_units = match body.get("MaxWriteRequestUnits").filter(|v| !v.is_null()) {
            None => None,
            Some(v) => match v.as_i64() {
                Some(n) if n >= 1024 => Some(n),
                _ => {
                    return Err(constraint(
                        &v.to_string(),
                        "maxWriteRequestUnits",
                        "Member must have value greater than or equal to 1024",
                    ));
                }
            },
        };
        check_enum(&body, "PerformanceTier", &["STANDARD", "ACCELERATED"])?;
        let tier = body["PerformanceTier"].as_str().map(str::to_string);

        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let table = super::get_table(&state.tables, table_name)?;
        if table.status != "ACTIVE" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!(
                    "Table {} is in {} status; bulk operations need an ACTIVE table",
                    table.name, table.status
                ),
            ));
        }
        let (name, arn, table_id) = (
            table.name.clone(),
            table.arn.clone(),
            table.table_id.clone(),
        );

        // A retry with the same client token returns the operation it
        // started; the token with other parameters is a mismatch.
        if let Some(token) = client_token.as_deref() {
            let window = now - Duration::hours(IDEMPOTENCY_WINDOW_HOURS);
            if let Some(existing) = state
                .bulk_operations
                .values()
                .find(|op| op.client_token.as_deref() == Some(token) && op.start_time > window)
            {
                let same = existing.table_arn == arn
                    && existing.input_s3_configuration == input
                    && existing.output_s3_configuration == output
                    && existing.max_write_request_units == max_units
                    && existing.performance_tier == tier;
                if !same {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "IdempotentParameterMismatchException",
                        "The ClientToken was already used with different parameters",
                    ));
                }
                return Self::ok_json(
                    json!({ "BulkOperationDescription": description_json(existing) }),
                );
            }
        }

        let op = BulkOperation {
            arn: format!("{arn}/bulk-operation/{}", job_id(now)),
            status: "IN_PROGRESS".to_string(),
            table_name: name,
            table_arn: arn,
            table_id,
            client_token,
            input_s3_configuration: input,
            output_s3_configuration: output,
            max_write_request_units: max_units,
            performance_tier: tier,
            start_time: now,
            end_time: None,
            processed_item_count: 0,
            failed_item_count: 0,
            processed_input_bytes: 0,
            total_input_bytes: None,
            usage: BulkOperationUsage::default(),
            failure_code: None,
            failure_message: None,
            cancel_requested: false,
        };
        let job = bulk_job(&req.account_id, &req.region, &op);
        let response = json!({ "BulkOperationDescription": description_json(&op) });
        state.bulk_operations.insert(op.arn.clone(), op);
        drop(accounts);
        spawn_bulk(self.job_context(), job);
        Self::ok_json(response)
    }

    pub(super) fn describe_bulk_operation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let arn = require_str(&body, "BulkOperationArn")?;
        check_length(Some(arn), "bulkOperationArn", 37, 1024)?;
        let accounts = self.state.read();
        let op = accounts
            .regional(&req.account_id, &req.region)
            .and_then(|s| s.bulk_operations.get(arn))
            .ok_or_else(|| not_found(arn))?;
        Self::ok_json(json!({ "BulkOperationDescription": description_json(op) }))
    }

    pub(super) fn list_bulk_operations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let page_size = match body.get("MaxResults").filter(|v| !v.is_null()) {
            None => DEFAULT_PAGE_SIZE,
            Some(v) => match v.as_i64() {
                Some(n @ 1..=25) => n as usize,
                _ => {
                    return Err(constraint(
                        &v.to_string(),
                        "maxResults",
                        "Member must have value between 1 and 25",
                    ));
                }
            },
        };
        let start = match body["NextToken"].as_str() {
            Some(t) => parse_page_token(t)?,
            None => 0,
        };
        let cutoff = Utc::now() - Duration::days(LIST_WINDOW_DAYS);
        let accounts = self.state.read();
        let mut ops: Vec<&BulkOperation> = accounts
            .regional(&req.account_id, &req.region)
            .map(|s| s.bulk_operations.values().collect())
            .unwrap_or_default();
        ops.retain(|op| op.start_time >= cutoff);
        // Most recent first.
        ops.sort_by(|a, b| b.start_time.cmp(&a.start_time).then(b.arn.cmp(&a.arn)));
        let page: Vec<Value> = ops
            .iter()
            .skip(start)
            .take(page_size)
            .map(|op| {
                let mut s = json!({
                    "BulkOperationArn": op.arn,
                    "BulkOperationStatus": op.status,
                    "StartTime": epoch_secs(op.start_time),
                });
                if let Some(end) = op.end_time {
                    s["EndTime"] = json!(epoch_secs(end));
                }
                s
            })
            .collect();
        let mut resp = json!({ "BulkOperationsSummaryList": page });
        if start + page_size < ops.len() {
            resp["NextToken"] = json!(page_token(start + page_size));
        }
        Self::ok_json(resp)
    }

    pub(super) fn cancel_bulk_operation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let arn = require_str(&body, "BulkOperationArn")?;
        check_length(Some(arn), "bulkOperationArn", 37, 1024)?;
        let mut accounts = self.state.write();
        let op = accounts
            .regional_get_mut(&req.account_id, &req.region)
            .and_then(|s| s.bulk_operations.get_mut(arn))
            .ok_or_else(|| not_found(arn))?;
        match op.status.as_str() {
            "IN_PROGRESS" | "CANCELLING" => {
                op.cancel_requested = true;
                op.status = "CANCELLING".to_string();
            }
            other => {
                return Err(validation(format!(
                    "Bulk operation {arn} cannot be cancelled because it is {other}"
                )));
            }
        }
        Self::ok_json(json!({
            "BulkOperationArn": op.arn,
            "BulkOperationStatus": op.status,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SharedDynamoDbState;
    use fakecloud_s3::SharedS3State;
    use parking_lot::RwLock;
    use std::sync::Arc;

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
            body: bytes::Bytes::from(body.to_string()),
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

    fn call(svc: &DynamoDbService, action: &str, body: Value) -> Result<Value, AwsServiceError> {
        let resp = match action {
            "StartBulkPutItems" => svc.start_bulk_put_items(&request(action, body)),
            "DescribeBulkOperation" => svc.describe_bulk_operation(&request(action, body)),
            "ListBulkOperations" => svc.list_bulk_operations(&request(action, body)),
            "CancelBulkOperation" => svc.cancel_bulk_operation(&request(action, body)),
            "CreateTable" => svc.create_table(&request(action, body)),
            "GetItem" => svc.get_item(&request(action, body)),
            _ => unreachable!("{action}"),
        }?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
    }

    fn setup() -> (DynamoDbService, SharedS3State) {
        let state: SharedDynamoDbState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "us-east-1", ""),
        ));
        let s3: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "us-east-1", ""),
        ));
        for bucket in ["in", "out"] {
            s3.write().get_or_create(ACCOUNT).buckets.insert(
                bucket.to_string(),
                fakecloud_s3::S3Bucket::new(bucket, "us-east-1", "owner"),
            );
        }
        let svc = DynamoDbService::new(state).with_s3(s3.clone());
        call(
            &svc,
            "CreateTable",
            json!({
                "TableName": "Tbl",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "g", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "byG",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "KEYS_ONLY"}
                }],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .unwrap();
        (svc, s3)
    }

    fn put_object(s3: &SharedS3State, bucket: &str, key: &str, data: &[u8]) {
        s3.write()
            .get_or_create(ACCOUNT)
            .buckets
            .get_mut(bucket)
            .unwrap()
            .objects
            .insert(
                key.to_string(),
                fakecloud_s3::S3Object {
                    key: key.to_string(),
                    body: fakecloud_s3::memory_body(bytes::Bytes::from(data.to_vec())),
                    size: data.len() as u64,
                    ..Default::default()
                },
            );
    }

    fn get_object(s3: &SharedS3State, bucket: &str, key: &str) -> Option<Vec<u8>> {
        let guard = s3.read();
        let acct = guard.get(ACCOUNT)?;
        let obj = acct.buckets.get(bucket)?.objects.get(key)?;
        acct.read_body(&obj.body).ok().map(|b| b.to_vec())
    }

    fn start_body(extra: Value) -> Value {
        let mut body = json!({
            "TableName": "Tbl",
            "InputS3Configuration": {"S3Bucket": "in", "S3KeyPrefix": "data/", "InputFormat": "DYNAMODB_JSON"},
            "OutputS3Configuration": {"S3Bucket": "out", "S3KeyPrefix": "reports", "OutputType": ["ERROR"]},
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        body
    }

    fn describe(svc: &DynamoDbService, arn: &str) -> Value {
        call(
            svc,
            "DescribeBulkOperation",
            json!({"BulkOperationArn": arn}),
        )
        .unwrap()["BulkOperationDescription"]
            .clone()
    }

    #[test]
    fn bulk_put_writes_items_and_reports_errors() {
        let (svc, s3) = setup();
        put_object(
            &s3,
            "in",
            "data/1.json",
            b"{\"Item\":{\"pk\":{\"S\":\"a\"},\"g\":{\"S\":\"x\"},\"v\":{\"N\":\"1\"}}}\n\
              {\"Item\":{\"pk\":{\"S\":\"b\"}}}\n\
              {\"Item\":{\"nokey\":{\"S\":\"c\"}}}\n",
        );
        let gz = {
            use std::io::Write;
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(b"{\"Item\":{\"pk\":{\"S\":\"c\"}}}\nnot json\n")
                .unwrap();
            enc.finish().unwrap()
        };
        put_object(
            &s3,
            "in",
            "other/ignored.json",
            b"{\"Item\":{\"pk\":{\"S\":\"z\"}}}\n",
        );
        // Uncompressed run first.
        let started = call(
            &svc,
            "StartBulkPutItems",
            start_body(json!({"ClientToken": "tok"})),
        )
        .unwrap()["BulkOperationDescription"]
            .clone();
        let arn = started["BulkOperationArn"].as_str().unwrap().to_string();
        assert!(arn.contains(":table/Tbl/bulk-operation/"), "{arn}");
        let d = describe(&svc, &arn);
        assert_eq!(d["BulkOperationStatus"], "COMPLETED_WITH_ERRORS");
        assert_eq!(d["ProcessedItemCount"], 3);
        assert_eq!(d["FailedItemCount"], 1);
        assert_eq!(d["TableDetails"][0]["TableName"], "Tbl");
        assert_eq!(d["InputS3Configuration"]["InputCompressionType"], "NONE");
        assert_eq!(d["PerformanceTier"], "STANDARD");
        assert!(d["EndTime"].is_number());
        assert!(d["TotalInputBytes"].as_i64().unwrap() > 0);
        assert_eq!(d["ProcessedInputBytes"], d["TotalInputBytes"]);
        let usage = &d["BulkOperationUsage"][0];
        assert!(usage["Table"]["BulkPutBytes"].as_i64().unwrap() > 0);
        // Only item `a` carries the index key.
        assert!(usage["GlobalSecondaryIndexes"]["byG"].as_i64().unwrap() > 0);
        let got = call(
            &svc,
            "GetItem",
            json!({"TableName": "Tbl", "Key": {"pk": {"S": "b"}}}),
        )
        .unwrap();
        assert_eq!(got["Item"]["pk"]["S"], "b");
        let missing = call(
            &svc,
            "GetItem",
            json!({"TableName": "Tbl", "Key": {"pk": {"S": "z"}}}),
        )
        .unwrap();
        assert!(missing.get("Item").is_none());
        let op_id = arn.rsplit('/').next().unwrap();
        let report = get_object(
            &s3,
            "out",
            &format!("reports/AWSDynamoDB/BulkOperations/{op_id}/errors/part-00000.json"),
        )
        .expect("error report written");
        let line: Value =
            serde_json::from_str(String::from_utf8(report).unwrap().lines().next().unwrap())
                .unwrap();
        assert_eq!(line["InputObjectKey"], "data/1.json");
        assert_eq!(line["RecordNumber"], 3);

        // The same token and parameters return the same operation; other
        // parameters are a mismatch.
        let again = call(
            &svc,
            "StartBulkPutItems",
            start_body(json!({"ClientToken": "tok"})),
        )
        .unwrap();
        assert_eq!(
            again["BulkOperationDescription"]["BulkOperationArn"],
            arn.as_str()
        );
        let err = call(
            &svc,
            "StartBulkPutItems",
            start_body(json!({"ClientToken": "tok", "PerformanceTier": "ACCELERATED"})),
        )
        .unwrap_err();
        assert_eq!(err.code(), "IdempotentParameterMismatchException");

        // A gzip run reports its unparsable record.
        put_object(&s3, "in", "gz/1.json.gz", &gz);
        let mut body = start_body(json!({}));
        body["InputS3Configuration"]["S3KeyPrefix"] = json!("gz/");
        body["InputS3Configuration"]["InputCompressionType"] = json!("GZIP");
        let arn2 = call(&svc, "StartBulkPutItems", body).unwrap()["BulkOperationDescription"]
            ["BulkOperationArn"]
            .as_str()
            .unwrap()
            .to_string();
        let d = describe(&svc, &arn2);
        assert_eq!(d["ProcessedItemCount"], 2);
        assert_eq!(d["FailedItemCount"], 1);

        // Newest first, paginated with hex tokens.
        let page = call(&svc, "ListBulkOperations", json!({"MaxResults": 1})).unwrap();
        assert_eq!(
            page["BulkOperationsSummaryList"][0]["BulkOperationArn"],
            arn2.as_str()
        );
        let token = page["NextToken"].as_str().unwrap().to_string();
        assert_eq!(token.len(), 16);
        let page2 = call(&svc, "ListBulkOperations", json!({"NextToken": token})).unwrap();
        assert_eq!(
            page2["BulkOperationsSummaryList"][0]["BulkOperationArn"],
            arn.as_str()
        );
        assert!(page2.get("NextToken").is_none());
        assert_eq!(
            call(&svc, "ListBulkOperations", json!({"NextToken": "xyz"}))
                .unwrap_err()
                .code(),
            "ValidationException"
        );

        // A finished operation cannot be cancelled.
        assert_eq!(
            call(
                &svc,
                "CancelBulkOperation",
                json!({"BulkOperationArn": arn})
            )
            .unwrap_err()
            .code(),
            "ValidationException"
        );
    }

    #[test]
    fn start_is_validated_against_the_table_and_request() {
        let (svc, _s3) = setup();
        let mut no_table = start_body(json!({}));
        no_table["TableName"] = json!("Nope");
        assert_eq!(
            call(&svc, "StartBulkPutItems", no_table)
                .unwrap_err()
                .code(),
            "ResourceNotFoundException"
        );
        let mut bad_format = start_body(json!({}));
        bad_format["InputS3Configuration"]["InputFormat"] = json!("CSV");
        assert_eq!(
            call(&svc, "StartBulkPutItems", bad_format)
                .unwrap_err()
                .code(),
            "ValidationException"
        );
        assert_eq!(
            call(
                &svc,
                "StartBulkPutItems",
                start_body(json!({"MaxWriteRequestUnits": 10}))
            )
            .unwrap_err()
            .code(),
            "ValidationException"
        );
        let mut no_types = start_body(json!({}));
        no_types["OutputS3Configuration"]["OutputType"] = json!([]);
        assert_eq!(
            call(&svc, "StartBulkPutItems", no_types)
                .unwrap_err()
                .code(),
            "ValidationException"
        );
        let missing_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl/bulk-operation/nope";
        for op in ["DescribeBulkOperation", "CancelBulkOperation"] {
            assert_eq!(
                call(&svc, op, json!({"BulkOperationArn": missing_arn}))
                    .unwrap_err()
                    .code(),
                "ResourceNotFoundException"
            );
        }
    }

    #[test]
    fn missing_input_bucket_fails_the_operation() {
        let (svc, _s3) = setup();
        let mut body = start_body(json!({}));
        body["InputS3Configuration"]["S3Bucket"] = json!("absent");
        let arn = call(&svc, "StartBulkPutItems", body).unwrap()["BulkOperationDescription"]
            ["BulkOperationArn"]
            .as_str()
            .unwrap()
            .to_string();
        let d = describe(&svc, &arn);
        assert_eq!(d["BulkOperationStatus"], "FAILED");
        assert_eq!(d["FailureCode"], "S3NoSuchBucket");
    }

    #[test]
    fn cancelling_stops_the_job_before_its_next_object() {
        let (svc, s3) = setup();
        put_object(
            &s3,
            "in",
            "data/1.json",
            b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n",
        );
        // Record an in-progress operation without running it, cancel it, then
        // let the job run.
        let now = Utc::now();
        let op = BulkOperation {
            arn: format!(
                "arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/Tbl/bulk-operation/{}",
                job_id(now)
            ),
            status: "IN_PROGRESS".to_string(),
            table_name: "Tbl".to_string(),
            table_arn: format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/Tbl"),
            table_id: svc
                .state
                .read()
                .regional(ACCOUNT, "us-east-1")
                .unwrap()
                .tables["Tbl"]
                .table_id
                .clone(),
            client_token: None,
            input_s3_configuration: json!({"S3Bucket": "in", "S3KeyPrefix": "data/", "InputFormat": "DYNAMODB_JSON"}),
            output_s3_configuration: json!({"S3Bucket": "out", "OutputType": ["ERROR"]}),
            max_write_request_units: None,
            performance_tier: None,
            start_time: now,
            end_time: None,
            processed_item_count: 0,
            failed_item_count: 0,
            processed_input_bytes: 0,
            total_input_bytes: None,
            usage: BulkOperationUsage::default(),
            failure_code: None,
            failure_message: None,
            cancel_requested: false,
        };
        let arn = op.arn.clone();
        let job = bulk_job(ACCOUNT, "us-east-1", &op);
        svc.state
            .write()
            .regional_mut(ACCOUNT, "us-east-1")
            .bulk_operations
            .insert(arn.clone(), op);
        let resp = call(
            &svc,
            "CancelBulkOperation",
            json!({"BulkOperationArn": arn}),
        )
        .unwrap();
        assert_eq!(resp["BulkOperationStatus"], "CANCELLING");
        assert_eq!(describe(&svc, &arn)["BulkOperationStatus"], "CANCELLING");
        run_bulk(&svc.job_context(), &job);
        let d = describe(&svc, &arn);
        assert_eq!(d["BulkOperationStatus"], "CANCELLED");
        assert_eq!(d["FailureCode"], CANCELLED_CODE);
        assert_eq!(d["ProcessedItemCount"], 0);
        let got = call(
            &svc,
            "GetItem",
            json!({"TableName": "Tbl", "Key": {"pk": {"S": "a"}}}),
        )
        .unwrap();
        assert!(got.get("Item").is_none());
    }

    #[test]
    fn resume_restarts_in_progress_and_settles_cancelling_operations() {
        let (svc, s3) = setup();
        put_object(
            &s3,
            "in",
            "data/1.json",
            b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n",
        );
        let arn = call(&svc, "StartBulkPutItems", start_body(json!({}))).unwrap()
            ["BulkOperationDescription"]["BulkOperationArn"]
            .as_str()
            .unwrap()
            .to_string();
        let mut cancelling = svc
            .state
            .read()
            .regional(ACCOUNT, "us-east-1")
            .unwrap()
            .bulk_operations[&arn]
            .clone();
        cancelling.arn = format!("{arn}-c");
        cancelling.status = "CANCELLING".to_string();
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut(ACCOUNT, "us-east-1");
            let op = state.bulk_operations.get_mut(&arn).unwrap();
            op.status = "IN_PROGRESS".to_string();
            op.end_time = None;
            state
                .bulk_operations
                .insert(cancelling.arn.clone(), cancelling.clone());
        }
        svc.resume_interrupted_jobs();
        let d = describe(&svc, &arn);
        assert_eq!(d["BulkOperationStatus"], "COMPLETED");
        assert_eq!(d["ProcessedItemCount"], 1);
        assert_eq!(
            describe(&svc, &cancelling.arn)["BulkOperationStatus"],
            "CANCELLED"
        );
    }
}
