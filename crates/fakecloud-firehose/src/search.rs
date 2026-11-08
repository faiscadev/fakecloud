//! Elasticsearch / OpenSearch Service destinations: records are indexed with
//! the `_bulk` API at `ClusterEndpoint`, and the destination's
//! `S3Configuration` receives failed (or all) documents.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;

use base64::Engine;
use chrono::{DateTime, Datelike, Utc};
use fakecloud_core::service::AwsServiceError;
use serde_json::{json, Value};

use crate::admission::unsupported;
use crate::config::{buffering_policy, delivery_config, invalid_argument, parse_buffering_hints};
use crate::writer::DeliveryBatch;
use crate::DeliveryConfig;

pub(crate) const ELASTICSEARCH: &str = "ElasticsearchDestinationDescription";
pub(crate) const OPENSEARCH: &str = "AmazonopensearchserviceDestinationDescription";

#[derive(Clone, Debug)]
pub struct SearchDelivery {
    pub endpoint: String,
    pub index_name: String,
    pub rotation: String,
    pub type_name: Option<String>,
    pub document_ids: bool,
    pub backup_all: bool,
    pub retry: Duration,
    /// Where failed documents go: `ErrorOutputPrefix`, else the backup
    /// prefix plus the destination's `*-failed/` folder.
    pub error_prefix: String,
    /// Bulk requests made for the batch this configuration was captured for.
    pub attempts: Arc<BulkAttempts>,
}

/// Bulk requests made for one batch. The retry duration runs from the first
/// request, so time a batch spends queued behind a failing one does not use it.
#[derive(Debug, Default)]
pub struct BulkAttempts {
    first: OnceLock<DateTime<Utc>>,
    made: AtomicU64,
    progress: Mutex<Option<BulkProgress>>,
}

/// What retries of a batch still have to do. Documents that were indexed or
/// finally rejected are not sent again.
#[derive(Clone, Debug, Default)]
pub(crate) struct BulkProgress {
    pub rejected: Vec<Failure>,
    pub pending: Vec<usize>,
}

impl BulkAttempts {
    /// Records a new request and returns when the first one started.
    pub(crate) fn begin(&self) -> DateTime<Utc> {
        self.made.fetch_add(1, Ordering::Relaxed);
        *self.first.get_or_init(Utc::now)
    }

    pub(crate) fn made(&self) -> u64 {
        self.made.load(Ordering::Relaxed)
    }

    /// Progress so far, starting with the records no cluster could index.
    pub(crate) fn progress(&self, batch: &DeliveryBatch) -> BulkProgress {
        self.progress
            .lock()
            .get_or_insert_with(|| classify_records(batch))
            .clone()
    }

    pub(crate) fn save(&self, progress: BulkProgress) {
        *self.progress.lock() = Some(progress);
    }
}

/// A bulk request that failed as a whole, with its Firehose error code.
#[derive(Debug, PartialEq)]
pub(crate) struct BulkError {
    pub code: &'static str,
    pub message: String,
}

/// A document Firehose could not index, with the error that rejected it.
#[derive(Clone, Debug)]
pub(crate) struct Failure {
    pub index: usize,
    pub code: String,
    pub message: String,
}

impl Failure {
    /// A cluster that is overloaded or failing may accept the document later,
    /// so it is resent for the retry duration like a failed request.
    pub(crate) fn retriable(&self) -> bool {
        self.code
            .parse::<u64>()
            .is_ok_and(|status| status == 429 || status >= 500)
    }
}

/// A `_bulk` body is line-delimited, so a record that is not one line of JSON
/// would corrupt the framing of every document after it. Only that record fails.
fn classify_records(batch: &DeliveryBatch) -> BulkProgress {
    let mut progress = BulkProgress::default();
    for (index, record) in batch.records.iter().enumerate() {
        let line = record.trim_ascii_end();
        let single_json_object = !line.contains(&b'\n')
            && serde_json::from_slice::<Value>(line).is_ok_and(|value| value.is_object());
        if single_json_object {
            progress.pending.push(index);
        } else {
            progress.rejected.push(Failure {
                index,
                code: "OS.MalformedData".into(),
                message: "One or more records are malformed. Please ensure that each record is \
                          single valid JSON object and that it does not contain newlines."
                    .into(),
            });
        }
    }
    progress
}

pub(crate) fn delivery(kind: &str, config: &Value) -> Result<DeliveryConfig, AwsServiceError> {
    let endpoint = config["ClusterEndpoint"]
        .as_str()
        .filter(|endpoint| !endpoint.is_empty())
        .ok_or_else(|| unsupported("a DomainARN without ClusterEndpoint"))?;
    let index_name = config["IndexName"]
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| invalid_argument("IndexName is required"))?;
    let rotation = config["IndexRotationPeriod"].as_str().unwrap_or("OneDay");
    if !matches!(
        rotation,
        "NoRotation" | "OneHour" | "OneDay" | "OneWeek" | "OneMonth"
    ) {
        return Err(invalid_argument(format!(
            "Invalid IndexRotationPeriod: {rotation}"
        )));
    }
    if config["ProcessingConfiguration"]["Enabled"].as_bool() == Some(true) {
        return Err(unsupported("ProcessingConfiguration"));
    }
    let backup_all = match config["S3BackupMode"].as_str() {
        None | Some("FailedDocumentsOnly") => false,
        Some("AllDocuments") => true,
        Some(mode) => return Err(invalid_argument(format!("Invalid S3BackupMode: {mode}"))),
    };
    let s3 = crate::config::parse_s3_destination(&config["S3Configuration"])?
        .ok_or_else(|| invalid_argument("S3Configuration is required"))?;
    let mut delivery = delivery_config(&s3)?;
    let (size, interval) = parse_buffering_hints(config)?;
    let size = size.unwrap_or(5);
    if size > 100 {
        return Err(invalid_argument(format!(
            "BufferingHints.SizeInMBs must be between 1 and 100, got {size}"
        )));
    }
    delivery.buffering = buffering_policy(Some(size), Some(interval.unwrap_or(300)))?;
    let failed_folder = if kind == OPENSEARCH {
        "AmazonOpenSearchService_failed/"
    } else {
        "elasticsearch-failed/"
    };
    if s3
        .error_output_prefix
        .as_deref()
        .is_some_and(|prefix| prefix.contains("!{"))
    {
        return Err(unsupported("ErrorOutputPrefix expressions"));
    }
    let error_prefix = match s3.error_output_prefix.as_deref() {
        Some(prefix) if !prefix.is_empty() => prefix.to_owned(),
        _ => format!("{}{failed_folder}", delivery.output.prefix),
    };
    delivery.search = Some(SearchDelivery {
        endpoint: endpoint.trim_end_matches('/').to_owned(),
        index_name: index_name.to_owned(),
        rotation: rotation.to_owned(),
        type_name: config["TypeName"]
            .as_str()
            .filter(|name| !name.is_empty())
            .map(str::to_owned),
        document_ids: config["DocumentIdOptions"]["DefaultDocumentIdFormat"] != "NO_DOCUMENT_ID",
        backup_all,
        retry: Duration::from_secs(
            config["RetryOptions"]["DurationInSeconds"]
                .as_u64()
                .unwrap_or(300),
        ),
        error_prefix,
        attempts: Arc::default(),
    });
    Ok(delivery)
}

pub(crate) fn index_name(base: &str, rotation: &str, at: DateTime<Utc>) -> String {
    let suffix = match rotation {
        "OneHour" => at.format("-%Y-%m-%d-%H").to_string(),
        "OneDay" => at.format("-%Y-%m-%d").to_string(),
        "OneWeek" => week_suffix(at),
        "OneMonth" => at.format("-%Y-%m").to_string(),
        _ => String::new(),
    };
    format!("{base}{suffix}")
}

/// AWS numbers weeks by US convention: they start on Sunday, and week 1 is the
/// first one containing a Saturday of the year. A week therefore belongs to the
/// year of its Saturday, even when it starts in December.
fn week_suffix(at: DateTime<Utc>) -> String {
    let days_to_saturday = 6 - i64::from(at.weekday().num_days_from_sunday());
    let saturday = at.date_naive() + chrono::Duration::days(days_to_saturday);
    format!("-{}-w{:02}", saturday.year(), saturday.ordinal0() / 7 + 1)
}

fn document_id(batch: &DeliveryBatch, position: usize) -> String {
    if let Some(id) = batch.document_ids.get(position) {
        return id.clone();
    }
    // Stable across retries of the same batch, so a retried bulk request
    // overwrites the documents it already indexed instead of duplicating them.
    let object = batch.object_key.rsplit('/').next().unwrap_or_default();
    format!("{object}.{position}")
}

pub(crate) fn bulk_body(
    batch: &DeliveryBatch,
    search: &SearchDelivery,
    index: &str,
    positions: &[usize],
) -> Vec<u8> {
    let mut body = Vec::new();
    for &position in positions {
        let record = &batch.records[position];
        let mut action = json!({ "_index": index });
        if let Some(type_name) = &search.type_name {
            action["_type"] = json!(type_name);
        }
        if search.document_ids {
            action["_id"] = json!(document_id(batch, position));
        }
        body.extend_from_slice(json!({ "index": action }).to_string().as_bytes());
        body.push(b'\n');
        body.extend_from_slice(record.trim_ascii_end());
        body.push(b'\n');
    }
    body
}

/// Per-document failures in a `_bulk` response to the records at `sent`, naming
/// those records. A response that cannot be matched to the request is an error, not a
/// success: treating it as one would drop documents without a failure record.
pub(crate) fn item_failures(response: &Value, sent: &[usize]) -> Result<Vec<Failure>, BulkError> {
    let malformed = |message: &str| BulkError {
        code: "OS.ClusterError",
        message: format!("bulk response {message}"),
    };
    let errors = response["errors"]
        .as_bool()
        .ok_or_else(|| malformed("has no errors member"))?;
    let items = response["items"].as_array();
    if let Some(items) = items.filter(|items| items.len() != sent.len()) {
        return Err(BulkError {
            code: "OS.ResponseEntryCountMismatch",
            message: format!(
                "bulk response has {} entries for {} records",
                items.len(),
                sent.len()
            ),
        });
    }
    if !errors {
        return Ok(Vec::new());
    }
    let items = items.ok_or_else(|| malformed("reports errors but has no items"))?;
    let mut failures = Vec::new();
    if items.len() != sent.len() {
        return Err(malformed("is missing items"));
    }
    for (&index, item) in sent.iter().zip(items) {
        let result = item
            .as_object()
            .and_then(|item| item.values().next())
            .ok_or_else(|| malformed("has an item without a result"))?;
        let status = result["status"]
            .as_u64()
            .ok_or_else(|| malformed("has an item without a status"))?;
        if status >= 300 {
            failures.push(Failure {
                index,
                code: status.to_string(),
                message: result["error"].to_string(),
            });
        }
    }
    Ok(failures)
}

fn status_error_code(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() {
        401 | 403 => "OS.AccessDenied",
        404 => "OS.ResourceNotFound",
        502 => "OS.BadGateway",
        504 => "OS.GatewayTimeout",
        _ => "OS.ClusterError",
    }
}

fn send_error_code(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "OS.RequestTimeout"
    } else if error.is_connect() {
        "OS.ConnectionFailed"
    } else {
        "OS.ConnectionReset"
    }
}

pub(crate) fn post_bulk(
    search: &SearchDelivery,
    index: &str,
    batch: &DeliveryBatch,
    positions: &[usize],
) -> Result<Vec<Failure>, BulkError> {
    let url = format!("{}/_bulk", search.endpoint);
    let body = bulk_body(batch, search, index, positions);
    tokio::runtime::Handle::current().block_on(async {
        let response = reqwest::Client::new()
            .post(&url)
            .header("content-type", "application/json")
            .timeout(Duration::from_secs(60))
            .body(body)
            .send()
            .await
            .map_err(|e| BulkError {
                code: send_error_code(&e),
                message: format!("bulk request to {url} failed: {e}"),
            })?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(BulkError {
                code: status_error_code(status),
                message: format!("bulk request to {url} returned {status}: {text}"),
            });
        }
        let parsed: Value = serde_json::from_str(&text).map_err(|e| BulkError {
            code: "OS.ClusterError",
            message: format!("bulk response from {url} is not JSON: {e}"),
        })?;
        item_failures(&parsed, positions)
    })
}

/// One line of the failed-documents backup object, in Firehose's format.
pub(crate) fn failed_record(
    batch: &DeliveryBatch,
    search: &SearchDelivery,
    index: &str,
    failure: &Failure,
) -> Vec<u8> {
    let arrival = batch
        .arrivals
        .get(failure.index)
        .unwrap_or(&batch.oldest_arrival);
    let mut line = json!({
        "attemptsMade": search.attempts.made(),
        "arrivalTimestamp": arrival.timestamp_millis(),
        "errorCode": failure.code,
        "errorMessage": failure.message,
        "attemptEndingTimestamp": Utc::now().timestamp_millis(),
        "rawData": base64::engine::general_purpose::STANDARD.encode(&batch.records[failure.index]),
        "esDocumentId": if search.document_ids { json!(document_id(batch, failure.index)) } else { Value::Null },
        "esIndexName": index,
        "esTypeName": search.type_name.as_deref().unwrap_or_default(),
    })
    .to_string()
    .into_bytes();
    line.push(b'\n');
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_suffixes() {
        let at = DateTime::parse_from_rfc3339("2026-02-25T13:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(index_name("activity", "NoRotation", at), "activity");
        assert_eq!(
            index_name("activity", "OneHour", at),
            "activity-2026-02-25-13"
        );
        assert_eq!(index_name("activity", "OneDay", at), "activity-2026-02-25");
        assert_eq!(index_name("activity", "OneWeek", at), "activity-2026-w09");
        assert_eq!(index_name("activity", "OneMonth", at), "activity-2026-02");
    }

    #[test]
    fn week_rotation_follows_the_us_convention_across_years() {
        let week = |date: &str| {
            let at = DateTime::parse_from_rfc3339(&format!("{date}T12:00:00Z"))
                .unwrap()
                .with_timezone(&Utc);
            index_name("i", "OneWeek", at)
        };
        // 2027-01-01 is a Friday: week 1 of 2027, not ISO's 2026-w53.
        assert_eq!(week("2027-01-01"), "i-2027-w01");
        // Its week starts on Sunday 2026-12-27 and belongs to 2027.
        assert_eq!(week("2026-12-27"), "i-2027-w01");
        assert_eq!(week("2026-12-26"), "i-2026-w52");
        // 2023-01-01 is a Sunday, so week 1 starts that day.
        assert_eq!(week("2023-01-01"), "i-2023-w01");
        assert_eq!(week("2023-01-08"), "i-2023-w02");
    }

    #[test]
    fn item_failures_keep_request_positions() {
        let response = json!({"errors": true, "items": [
            {"index": {"status": 201}},
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
        ]});
        let failures = item_failures(&response, &[0, 3]).unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].index, 3, "the position in the batch");
        assert_eq!(failures[0].code, "400");
        assert!(failures[0].message.contains("mapper_parsing_exception"));
        assert!(item_failures(&json!({"errors": false, "items": []}), &[])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unmatched_bulk_responses_are_errors_not_successes() {
        let item = |status: i64| json!({"index": {"status": status}});
        let code = |response: Value, sent: usize| {
            let positions: Vec<usize> = (0..sent).collect();
            item_failures(&response, &positions).err().unwrap().code
        };
        // More entries than records used to index past the batch and panic.
        assert_eq!(
            code(json!({"errors": true, "items": [item(201), item(400)]}), 1),
            "OS.ResponseEntryCountMismatch"
        );
        assert_eq!(
            code(json!({"errors": false, "items": [item(201)]}), 2),
            "OS.ResponseEntryCountMismatch"
        );
        assert_eq!(code(json!({}), 1), "OS.ClusterError");
        assert_eq!(code(json!({"errors": true}), 1), "OS.ClusterError");
        assert_eq!(
            code(json!({"errors": true, "items": [{"index": {}}]}), 1),
            "OS.ClusterError"
        );
        assert_eq!(
            code(json!({"errors": true, "items": [7]}), 1),
            "OS.ClusterError"
        );
    }

    #[test]
    fn only_overload_and_server_item_statuses_are_retriable() {
        let failure = |code: &str| Failure {
            index: 0,
            code: code.into(),
            message: String::new(),
        };
        for code in ["429", "500", "503"] {
            assert!(failure(code).retriable(), "{code}");
        }
        for code in ["400", "409", "OS.MalformedData"] {
            assert!(!failure(code).retriable(), "{code}");
        }
    }

    #[test]
    fn http_status_maps_to_the_documented_error_code() {
        for (status, code) in [
            (403, "OS.AccessDenied"),
            (404, "OS.ResourceNotFound"),
            (500, "OS.ClusterError"),
            (502, "OS.BadGateway"),
            (504, "OS.GatewayTimeout"),
        ] {
            let status = reqwest::StatusCode::from_u16(status).unwrap();
            assert_eq!(status_error_code(status), code);
        }
    }

    #[test]
    fn failure_location_defaults_and_expressions() {
        let config = |error_prefix: &str| {
            json!({
                "IndexName": "activity",
                "ClusterEndpoint": "http://localhost:9200",
                "S3Configuration": {
                    "RoleARN": "r", "BucketARN": "arn:aws:s3:::b",
                    "Prefix": "p/", "ErrorOutputPrefix": error_prefix,
                },
            })
        };
        let prefix = |kind: &str| {
            delivery(kind, &config(""))
                .unwrap()
                .search
                .unwrap()
                .error_prefix
        };
        assert_eq!(prefix(OPENSEARCH), "p/AmazonOpenSearchService_failed/");
        assert_eq!(prefix(ELASTICSEARCH), "p/elasticsearch-failed/");
        // An unevaluated expression would otherwise become a literal key.
        let err = delivery(OPENSEARCH, &config("!{firehose:error-output-type}/")).unwrap_err();
        assert!(err.message().contains("ErrorOutputPrefix"));
    }

    #[test]
    fn domain_arn_only_is_unsupported() {
        let config = json!({
            "IndexName": "activity",
            "DomainARN": "arn:aws:es:us-east-1:123456789012:domain/d",
            "S3Configuration": {"RoleARN": "r", "BucketARN": "arn:aws:s3:::b"},
        });
        let err = delivery(ELASTICSEARCH, &config).unwrap_err();
        assert!(err.message().contains("ClusterEndpoint"));
    }

    #[test]
    fn buffering_size_is_capped_at_100_mib() {
        let config = |size: i64| {
            json!({
                "IndexName": "activity",
                "ClusterEndpoint": "http://localhost:9200",
                "BufferingHints": {"SizeInMBs": size, "IntervalInSeconds": 60},
                "S3Configuration": {"RoleARN": "r", "BucketARN": "arn:aws:s3:::b"},
            })
        };
        assert!(delivery(OPENSEARCH, &config(100)).is_ok());
        let err = delivery(OPENSEARCH, &config(101)).unwrap_err();
        assert!(err.message().contains("between 1 and 100"));
    }
}
