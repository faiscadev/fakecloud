//! Elasticsearch / OpenSearch Service destinations: records are indexed with
//! the `_bulk` API at `ClusterEndpoint` or at the reachable cluster of the
//! `DomainARN`, and the destination's `S3Configuration` receives failed (or all)
//! documents. Failure codes, messages, object keys and line shapes follow
//! recordings of real Firehose against an OpenSearch 2.11 domain.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;

use base64::Engine;
use chrono::{DateTime, Datelike, Utc};
use fakecloud_core::service::AwsServiceError;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};

use crate::config::{
    buffering_policy, delivery_config, invalid_argument, parse_buffering_hints, s3_bucket_name,
};
use crate::output::object_name;
use crate::state::S3Destination;
use crate::writer::DeliveryBatch;
use crate::DeliveryConfig;

pub(crate) const ELASTICSEARCH: &str = "ElasticsearchDestinationDescription";
pub(crate) const OPENSEARCH: &str = "AmazonopensearchserviceDestinationDescription";

/// Wait before the second bulk attempt; each later wait doubles. Not published
/// by AWS: chosen because attempts at 0, 4, 12, 28 and 60 s give the 5 attempts
/// recorded for `DurationInSeconds` 60, whole-request and item-level alike.
const FIRST_BULK_RETRY: Duration = Duration::from_secs(4);

/// The destination API a stream was configured through, which names its failure
/// folder, error codes and failure-record members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchFamily {
    OpenSearch,
    Elasticsearch,
}

impl SearchFamily {
    fn of(kind: &str) -> Self {
        if kind == OPENSEARCH {
            Self::OpenSearch
        } else {
            Self::Elasticsearch
        }
    }

    fn failed_folder(self) -> &'static str {
        match self {
            Self::OpenSearch => "AmazonOpenSearchService-failed/",
            Self::Elasticsearch => "elasticsearch-failed/",
        }
    }

    pub(crate) fn code(self, name: &str) -> String {
        match self {
            Self::OpenSearch => format!("OS.{name}"),
            Self::Elasticsearch => format!("ES.{name}"),
        }
    }

    /// Recorded for OpenSearch Service; the Elasticsearch wording is inferred.
    fn access_denied(self) -> &'static str {
        match self {
            Self::OpenSearch => {
                "Access was denied. Ensure that the access policy on the Amazon OpenSearch \
                 Service cluster grants access to the configured IAM role."
            }
            Self::Elasticsearch => {
                "Access was denied. Ensure that the access policy on the Elasticsearch cluster \
                 grants access to the configured IAM role."
            }
        }
    }

    /// Recorded for OpenSearch Service; the Elasticsearch wording is inferred.
    fn connection_reset(self) -> &'static str {
        match self {
            Self::OpenSearch => {
                "Trouble maintaining connection with the Amazon OpenSearch Service cluster or \
                 OpenSearch Serverless collection. Ensure that the cluster or collection is \
                 healthy and has sufficient capacity for the current workload."
            }
            Self::Elasticsearch => {
                "Trouble maintaining connection with the Elasticsearch cluster. Ensure that the \
                 cluster is healthy and has sufficient capacity for the current workload."
            }
        }
    }

    /// What a cluster that cannot be connected to fails with. Recorded for a domain
    /// deleted with records buffered.
    pub(crate) fn connection_error(self) -> BulkError {
        BulkError {
            code: self.code("ConnectionReset"),
            message: self.connection_reset().to_owned(),
        }
    }

    fn service_exception(self, error: &str) -> String {
        match self {
            Self::OpenSearch => format!(
                "Error received from the Amazon OpenSearch Service cluster or OpenSearch \
                 Serverless collection. If the cluster or collection is behind a VPC, ensure \
                 network configuration allows connectivity. {error}"
            ),
            Self::Elasticsearch => format!("Error received from Elasticsearch cluster. {error}"),
        }
    }

    /// Prefix of the document ID, index and type members of a failure record.
    fn member_prefix(self) -> &'static str {
        match self {
            Self::OpenSearch => "os",
            Self::Elasticsearch => "es",
        }
    }
}

/// Where a destination's `_bulk` requests go. A domain resolves on every attempt,
/// so one that becomes reachable or goes away is seen without an update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchTarget {
    Cluster(String),
    Domain(String),
}

#[derive(Clone, Debug)]
pub struct SearchDelivery {
    pub family: SearchFamily,
    pub target: SearchTarget,
    pub index_name: String,
    pub rotation: String,
    pub type_name: Option<String>,
    pub document_ids: bool,
    pub backup_all: bool,
    pub retry: Duration,
    /// Where failed documents go: `ErrorOutputPrefix`, else the backup
    /// prefix plus the destination's `*-failed/` folder. A batch cut for delivery
    /// holds the directory this evaluates to.
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
    /// The last attempt asked for the bulk request to be repeated, as opposed to
    /// a failed S3 write, which retries on Firehose's S3 interval.
    bulk_retry_due: AtomicBool,
    /// Rejected-before-send records land in their own object, with its own UUID.
    rejected_key: OnceLock<String>,
    progress: Mutex<Option<BulkProgress>>,
}

/// What retries of a batch still have to do. Documents that were indexed or
/// finally rejected are not sent again.
#[derive(Clone, Debug, Default)]
pub(crate) struct BulkProgress {
    /// Refused before sending: they never reach the cluster.
    pub rejected: Vec<Failure>,
    pub rejected_written: bool,
    /// Sent and finally failed.
    pub failed: Vec<Failure>,
    pub pending: Vec<usize>,
    /// Every 400 item of a batch carries the message of the first one.
    pub first_bad_request: Option<String>,
    /// A document of the batch was indexed.
    pub any_indexed: bool,
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

    pub(crate) fn set_bulk_retry_due(&self, due: bool) {
        self.bulk_retry_due.store(due, Ordering::Relaxed);
    }

    /// How long to wait before the next bulk attempt, when one is due: doubling
    /// from [`FIRST_BULK_RETRY`], and never past the end of the retry duration,
    /// where the last attempt runs.
    pub(crate) fn retry_delay(&self, retry: Duration) -> Option<Duration> {
        if !self.bulk_retry_due.load(Ordering::Relaxed) {
            return None;
        }
        let doublings = u32::try_from(self.made().saturating_sub(1))
            .unwrap_or(u32::MAX)
            .min(16);
        let delay = FIRST_BULK_RETRY * 2u32.pow(doublings);
        let elapsed = self
            .first
            .get()
            .and_then(|first| (Utc::now() - *first).to_std().ok())
            .unwrap_or_default();
        Some(delay.min(retry.saturating_sub(elapsed)))
    }

    fn rejected_key(&self, batch: &DeliveryBatch, search: &SearchDelivery) -> String {
        self.rejected_key
            .get_or_init(|| {
                let name = object_name(
                    &batch.stream.name,
                    &batch.version_id,
                    &batch.destination.output,
                    batch.delivery_time,
                    uuid::Uuid::new_v4(),
                );
                format!("{}{name}", search.error_prefix)
            })
            .clone()
    }
}

/// A bulk request that failed as a whole, with its Firehose error code.
#[derive(Debug, PartialEq)]
pub(crate) struct BulkError {
    pub code: String,
    pub message: String,
}

/// A document Firehose could not index, with the error that rejected it.
#[derive(Clone, Debug)]
pub(crate) struct Failure {
    pub index: usize,
    pub code: String,
    pub message: String,
    /// Sent again until the retry duration ends. A 400 item is final; every
    /// other item status is resent (recorded for 403, inferred for the rest).
    pub retriable: bool,
    /// The cluster refused this document with status 400.
    pub bad_request_item: bool,
}

impl SearchDelivery {
    /// The directory a batch's failures go to, once its oldest arrival is known.
    /// Expressions make the whole directory; a static prefix is followed by the
    /// date hierarchy.
    pub(crate) fn failure_directory(&self, oldest_arrival: DateTime<Utc>) -> String {
        if crate::prefix::has_expressions(&self.error_prefix) {
            let folder = self.family.failed_folder().trim_end_matches('/');
            crate::prefix::render(&self.error_prefix, oldest_arrival, Some(folder))
        } else {
            format!(
                "{}{}/",
                self.error_prefix,
                oldest_arrival.format("%Y/%m/%d/%H")
            )
        }
    }
}

/// Recorded on AWS with 60 s retries: a batch of one good and one bad document
/// fails with this, while the same bad document alone, two bad ones, and every
/// batch that also held a record refused before sending fail with
/// `ServiceException`. "Part of the batch was indexed and nothing was refused"
/// fits all six recordings, but it is inferred, and only OpenSearch Service
/// destinations have shown it.
pub(crate) fn apply_parse_exception(family: SearchFamily, progress: &mut BulkProgress) {
    if family != SearchFamily::OpenSearch || !progress.any_indexed || !progress.rejected.is_empty()
    {
        return;
    }
    for failure in progress.failed.iter_mut().filter(|f| f.bad_request_item) {
        failure.code = family.code("AmazonOpenSearchServiceParseException");
        failure.message = "The Amazon OpenSearch Service cluster returned an \
                           AmazonOpenSearchServiceParseException. Ensure that the data being \
                           put is valid."
            .to_owned();
    }
}

/// How long a `ClusterEndpoint` check may hold up a request. The lookup and
/// connection run on their own thread so that a resolver or peer that never
/// answers cannot hang the request past this.
const ENDPOINT_PROBE_LIMIT: Duration = Duration::from_secs(3);

enum EndpointProbe {
    NoSuchName,
    Unreachable,
    Reachable,
}

fn probe_endpoint(host: String, port: u16) -> EndpointProbe {
    use std::net::{TcpStream, ToSocketAddrs};
    let Ok(addresses) = (host.as_str(), port).to_socket_addrs() else {
        return EndpointProbe::NoSuchName;
    };
    let reached = addresses
        .into_iter()
        .any(|address| TcpStream::connect_timeout(&address, ENDPOINT_PROBE_LIMIT).is_ok());
    if reached {
        EndpointProbe::Reachable
    } else {
        EndpointProbe::Unreachable
    }
}

/// Checks a `ClusterEndpoint` as AWS does when it is named: a name that does not
/// resolve and a cluster that cannot be connected to each have their own message.
pub(crate) fn probe_cluster_endpoint(endpoint: &str) -> Result<(), AwsServiceError> {
    let invalid = || {
        invalid_argument(format!(
            "Invalid Elasticsearch Cluster Endpoint {endpoint}. If this Elasticsearch domain is \
             in VPC, please specify VpcConfiguration."
        ))
    };
    let url = reqwest::Url::parse(endpoint).map_err(|_| invalid())?;
    let host = url
        .host_str()
        .ok_or_else(invalid)?
        .trim_matches(['[', ']'])
        .to_owned();
    let port = url.port_or_known_default().ok_or_else(invalid)?;
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(probe_endpoint(host, port));
    });
    match receiver.recv_timeout(ENDPOINT_PROBE_LIMIT) {
        Ok(EndpointProbe::Reachable) => Ok(()),
        Ok(EndpointProbe::NoSuchName) => Err(invalid()),
        Ok(EndpointProbe::Unreachable) | Err(_) => Err(invalid_argument(
            "Error connecting to the provided ElasticSearch cluster endpoint.  Ensure that the \
             ElasticSearch cluster endpoint is valid.",
        )),
    }
}

/// Only what cannot be a document at all is refused before sending. Anything
/// else goes to the cluster as it is: a non-object fails there as a parse error,
/// and an embedded newline breaks the framing of the whole request, as recorded.
fn classify_records(batch: &DeliveryBatch) -> BulkProgress {
    let mut progress = BulkProgress::default();
    for (index, record) in batch.records.iter().enumerate() {
        if batch.destination.expired.contains(&index) {
            continue;
        }
        let refusal = if record.is_empty() {
            Some((
                "EmptyRecordException",
                "com.amazonaws.firehose.v2.delivery.opensearch.processor.\
                 OpenSearchRecordsInMemoryRenderer$EmptyRecordException: The record did not \
                 contain any JSON documents",
            ))
        } else if std::str::from_utf8(record).is_err() {
            Some((
                "InvalidEncodingException",
                "com.amazonaws.firehose.v2.delivery.opensearch.processing.parser.\
                 NoModificationPayloadParser$InvalidEncodingException: The data could not be \
                 decoded as UTF-8",
            ))
        } else {
            None
        };
        match refusal {
            Some((code, message)) => progress.rejected.push(Failure {
                index,
                code: code.into(),
                message: message.into(),
                retriable: false,
                bad_request_item: false,
            }),
            None => progress.pending.push(index),
        }
    }
    progress
}

fn nonempty<'a>(config: &'a Value, member: &str) -> Option<&'a str> {
    config[member].as_str().filter(|value| !value.is_empty())
}

/// A member that defaults when absent, which a value of another type must not
/// pass for.
fn optional_string<'a>(
    config: &'a Value,
    member: &str,
) -> Result<Option<&'a str>, AwsServiceError> {
    match &config[member] {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(value)),
        other => Err(invalid_argument(format!(
            "{member} must be a string, got {other}"
        ))),
    }
}

/// What makes a stored destination invalid on AWS too, as opposed to what only
/// this emulator cannot deliver, which `delivery` refuses when records arrive.
fn validate_settings(config: &Value) -> Result<S3Destination, AwsServiceError> {
    if nonempty(config, "ClusterEndpoint").is_none() && nonempty(config, "DomainARN").is_none() {
        return Err(invalid_argument(
            "Provide either the Elasticsearch cluster endpoint or domain arn. Must specify \
             cluster endpoint for cross account Elasticsearch cluster.",
        ));
    }
    if nonempty(config, "IndexName").is_none() {
        return Err(invalid_argument("IndexName is required"));
    }
    let rotation = optional_string(config, "IndexRotationPeriod")?.unwrap_or("OneDay");
    if !matches!(
        rotation,
        "NoRotation" | "OneHour" | "OneDay" | "OneWeek" | "OneMonth"
    ) {
        return Err(invalid_argument(format!(
            "Invalid IndexRotationPeriod: {rotation}"
        )));
    }
    if !matches!(
        optional_string(config, "S3BackupMode")?,
        None | Some("FailedDocumentsOnly" | "AllDocuments")
    ) {
        return Err(invalid_argument(format!(
            "Invalid S3BackupMode: {}",
            config["S3BackupMode"]
        )));
    }
    let retry = &config["RetryOptions"]["DurationInSeconds"];
    if !retry.is_null() && !retry.as_u64().is_some_and(|seconds| seconds <= 7200) {
        return Err(invalid_argument(format!(
            "RetryOptions.DurationInSeconds must be between 0 and 7200, got {retry}"
        )));
    }
    let (size, _) = parse_buffering_hints(config)?;
    if let Some(size) = size.filter(|size| *size > 100) {
        return Err(invalid_argument(format!(
            "BufferingHints.SizeInMBs must be between 1 and 100, got {size}"
        )));
    }
    let s3 = crate::config::parse_s3_destination(&config["S3Configuration"])?
        .ok_or_else(|| invalid_argument("S3Configuration is required"))?;
    s3_bucket_name(&s3)?;
    Ok(s3)
}

/// A destination as `UpdateDestination` would leave it.
///
/// Naming both targets is refused here and not in `validate_settings`, which
/// delivery also runs: a stream stored with both before this check existed keeps
/// delivering to its `ClusterEndpoint`, rather than stopping after an upgrade.
pub(crate) fn validate_stored(config: &Value) -> Result<(), AwsServiceError> {
    if nonempty(config, "ClusterEndpoint").is_some() && nonempty(config, "DomainARN").is_some() {
        return Err(invalid_argument(
            "Cannot provide both Elasticsearch cluster endpoint and domain ARN. Provide either \
             the cluster endpoint or domain arn. Must specify cluster endpoint for cross \
             account elasticsearch cluster.",
        ));
    }
    validate_settings(config).map(|_| ())
}

/// A destination a CloudFormation template declares, whose `RoleARN` is
/// required as well.
pub fn validate_search_destination(config: &Value) -> Result<(), AwsServiceError> {
    if nonempty(config, "RoleARN").is_none() {
        return Err(invalid_argument("RoleARN is required"));
    }
    validate_stored(config)
}

pub(crate) fn delivery(kind: &str, config: &Value) -> Result<DeliveryConfig, AwsServiceError> {
    let s3 = validate_settings(config)?;
    let target = match nonempty(config, "ClusterEndpoint") {
        Some(endpoint) => SearchTarget::Cluster(endpoint.trim_end_matches('/').to_owned()),
        None => SearchTarget::Domain(nonempty(config, "DomainARN").unwrap_or_default().to_owned()),
    };
    let index_name = nonempty(config, "IndexName").unwrap_or_default();
    let rotation = config["IndexRotationPeriod"].as_str().unwrap_or("OneDay");
    if config["ProcessingConfiguration"]["Enabled"].as_bool() == Some(true) {
        return Err(crate::admission::unsupported("ProcessingConfiguration"));
    }
    let backup_all = config["S3BackupMode"] == "AllDocuments";
    let mut delivery = delivery_config(&s3)?;
    let (size, interval) = parse_buffering_hints(config)?;
    delivery.buffering = buffering_policy(Some(size.unwrap_or(5)), Some(interval.unwrap_or(300)))?;
    let family = SearchFamily::of(kind);
    if s3
        .error_output_prefix
        .as_deref()
        .is_some_and(crate::prefix::has_partition_keys)
    {
        return Err(crate::admission::unsupported(
            "ErrorOutputPrefix partition key expressions",
        ));
    }
    let error_prefix = match s3.error_output_prefix.as_deref() {
        Some(prefix) if !prefix.is_empty() => prefix.to_owned(),
        _ => format!("{}{}", delivery.output.prefix, family.failed_folder()),
    };
    delivery.search = Some(SearchDelivery {
        family,
        target,
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

fn arrival(batch: &DeliveryBatch, position: usize) -> DateTime<Utc> {
    batch
        .arrivals
        .get(position)
        .copied()
        .unwrap_or(batch.oldest_arrival)
}

/// The one place that decides which index a record goes to: per record, from its
/// own arrival time, as recorded for Kinesis and Direct PUT sources. Replays can
/// batch records differently, and a stable `_id` only overwrites within the same
/// index.
fn record_index(batch: &DeliveryBatch, search: &SearchDelivery, position: usize) -> String {
    index_name(
        &search.index_name,
        &search.rotation,
        arrival(batch, position),
    )
}

/// A 56-digit sequence-looking prefix shared by the records of one Direct PUT
/// call, whose IDs are `<prefix>.<subsequence>`.
pub(crate) fn direct_put_id_prefix() -> String {
    format!("49{:054}", uuid::Uuid::new_v4().as_u128())
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
    positions: &[usize],
) -> Vec<u8> {
    let mut body = Vec::new();
    for &position in positions {
        let record = &batch.records[position];
        let mut action = json!({ "_index": record_index(batch, search, position) });
        if let Some(type_name) = &search.type_name {
            action["_type"] = json!(type_name);
        }
        if search.document_ids {
            action["_id"] = json!(document_id(batch, position));
        }
        body.extend_from_slice(json!({ "index": action }).to_string().as_bytes());
        body.push(b'\n');
        // Not sanitised: a newline inside a record reaches the cluster, which
        // refuses the whole request, as on AWS. Only the last line needs ending.
        body.extend_from_slice(record);
        if !record.ends_with(b"\n") {
            body.push(b'\n');
        }
    }
    body
}

#[derive(Deserialize)]
struct BulkResponse {
    errors: bool,
    items: Vec<BTreeMap<String, BulkItem>>,
}

#[derive(Deserialize)]
struct BulkItem {
    status: u64,
    /// Kept as sent: failure records carry the cluster's bytes, member order
    /// included.
    error: Option<Box<RawValue>>,
}

/// Per-document failures in a `_bulk` response to the records at `sent`, naming
/// those records. A response that cannot be matched to the request is an error, not a
/// success: treating it as one would drop documents without a failure record.
pub(crate) fn item_failures(
    family: SearchFamily,
    response: &str,
    sent: &[usize],
    first_bad_request: &mut Option<String>,
) -> Result<Vec<Failure>, BulkError> {
    let parsed: BulkResponse = serde_json::from_str(response).map_err(|e| BulkError {
        code: family.code("ClusterError"),
        message: format!("bulk response could not be read: {e}"),
    })?;
    // The items decide, whatever `errors` says: a batch commits on them.
    let _ = parsed.errors;
    if parsed.items.len() != sent.len() {
        return Err(BulkError {
            code: family.code("ResponseEntryCountMismatch"),
            message: format!(
                "bulk response has {} entries for {} records",
                parsed.items.len(),
                sent.len()
            ),
        });
    }
    let mut failures = Vec::new();
    for (&index, item) in sent.iter().zip(&parsed.items) {
        let result = item.values().next().ok_or_else(|| BulkError {
            code: family.code("ClusterError"),
            message: "bulk response has an item without a result".to_owned(),
        })?;
        if result.status < 300 {
            continue;
        }
        let error = result.error.as_ref().map_or("null", |error| error.get());
        failures.push(if result.status == 400 {
            let first = first_bad_request.get_or_insert_with(|| error.to_owned());
            Failure {
                index,
                code: family.code("ServiceException"),
                message: family.service_exception(first),
                retriable: false,
                bad_request_item: true,
            }
        } else {
            Failure {
                index,
                code: result.status.to_string(),
                message: error.to_owned(),
                retriable: true,
                bad_request_item: false,
            }
        });
    }
    Ok(failures)
}

/// A 400 is what real clusters answer to a request that fails as a whole, and is
/// recorded. The other codes come from AWS's list of delivery errors; no response
/// carrying them has been recorded.
fn status_error(
    family: SearchFamily,
    status: reqwest::StatusCode,
    url: &str,
    body: &str,
) -> BulkError {
    let (name, message) = match status.as_u16() {
        400 => ("ServiceException", family.service_exception(body)),
        401 | 403 => ("AccessDenied", family.access_denied().to_owned()),
        code => (
            match code {
                404 => "ResourceNotFound",
                502 => "BadGateway",
                504 => "GatewayTimeout",
                _ => "ClusterError",
            },
            format!("bulk request to {url} returned {status}: {body}"),
        ),
    };
    BulkError {
        code: family.code(name),
        message,
    }
}

pub(crate) fn post_bulk(
    search: &SearchDelivery,
    endpoint: &str,
    batch: &DeliveryBatch,
    positions: &[usize],
    first_bad_request: &mut Option<String>,
) -> Result<Vec<Failure>, BulkError> {
    let family = search.family;
    let url = format!("{endpoint}/_bulk");
    let body = bulk_body(batch, search, positions);
    tokio::runtime::Handle::current().block_on(async {
        let response = reqwest::Client::new()
            .post(&url)
            .header("content-type", "application/json")
            .timeout(Duration::from_secs(60))
            .body(body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    BulkError {
                        code: family.code("RequestTimeout"),
                        message: format!("bulk request to {url} failed: {e}"),
                    }
                } else {
                    family.connection_error()
                }
            })?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(status_error(family, status, &url, &text));
        }
        item_failures(family, &text, positions, first_bad_request)
    })
}

fn json_string(value: &str) -> String {
    Value::from(value).to_string()
}

/// The members every failure record starts with, in Firehose's order. Written by
/// hand because `serde_json::Value` does not promise one.
fn failure_head(batch: &DeliveryBatch, failure: &Failure, attempts: u64, sub: u64) -> String {
    format!(
        "{{\"attemptsMade\":{attempts},\"arrivalTimestamp\":{},\"errorCode\":{},\
         \"errorMessage\":{},\"attemptEndingTimestamp\":{},\"rawData\":{},\
         \"subsequenceNumber\":{sub}",
        // Recorded arrival times have whole seconds.
        arrival(batch, failure.index).timestamp() * 1000,
        json_string(&failure.code),
        json_string(&failure.message),
        Utc::now().timestamp_millis(),
        json_string(
            &base64::engine::general_purpose::STANDARD.encode(&batch.records[failure.index])
        ),
    )
}

/// A record refused before sending: it has no document, index or type.
pub(crate) fn rejected_record(batch: &DeliveryBatch, failure: &Failure) -> String {
    let mut line = failure_head(batch, failure, 0, 0);
    line.push('}');
    line
}

/// A record the cluster finally refused.
pub(crate) fn failed_record(
    batch: &DeliveryBatch,
    search: &SearchDelivery,
    failure: &Failure,
) -> String {
    let id = document_id(batch, failure.index);
    let subsequence = id
        .rsplit_once('.')
        .and_then(|(_, sub)| sub.parse().ok())
        .unwrap_or(0);
    let member = search.family.member_prefix();
    format!(
        "{},\"{member}DocumentId\":{},\"{member}IndexName\":{},\"{member}TypeName\":{}}}",
        failure_head(batch, failure, search.attempts.made(), subsequence),
        if search.document_ids {
            json_string(&id)
        } else {
            "null".to_owned()
        },
        json_string(&record_index(batch, search, failure.index)),
        search
            .type_name
            .as_deref()
            .map_or("null".to_owned(), json_string),
    )
}

/// Where the records refused before sending go: their own object, written once.
pub(crate) fn rejected_object_key(batch: &DeliveryBatch, search: &SearchDelivery) -> String {
    search.attempts.rejected_key(batch, search)
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

    const OS: SearchFamily = SearchFamily::OpenSearch;

    fn failures(response: Value, sent: &[usize]) -> Result<Vec<Failure>, BulkError> {
        item_failures(OS, &response.to_string(), sent, &mut None)
    }

    #[test]
    fn item_failures_keep_request_positions() {
        let response = json!({"errors": true, "items": [
            {"index": {"status": 201}},
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
        ]});
        let found = failures(response, &[0, 3]).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].index, 3, "the position in the batch");
        assert_eq!(found[0].code, "OS.ServiceException");
        assert!(found[0].message.contains("mapper_parsing_exception"));
        assert!(failures(json!({"errors": false, "items": []}), &[])
            .unwrap()
            .is_empty());
    }

    /// As recorded: every 400 of a batch gets the first one's message, with the
    /// family's prefix and the cluster's bytes in the cluster's member order.
    #[test]
    fn bad_request_items_share_the_first_message_verbatim() {
        let first = r#"{"type":"mapper_parsing_exception","reason":"failed to parse","caused_by":{"type":"not_x_content_exception"}}"#;
        let response = format!(
            r#"{{"errors":true,"items":[{{"index":{{"status":400,"error":{first}}}}},{{"index":{{"status":400,"error":{{"type":"other"}}}}}}]}}"#
        );
        let mut first_bad = None;
        let found = item_failures(OS, &response, &[0, 1], &mut first_bad).unwrap();
        let expected = format!(
            "Error received from the Amazon OpenSearch Service cluster or OpenSearch Serverless \
             collection. If the cluster or collection is behind a VPC, ensure network \
             configuration allows connectivity. {first}"
        );
        assert_eq!(found[0].message, expected);
        assert_eq!(found[1].message, expected);
        assert!(!found[0].retriable);

        let found =
            item_failures(SearchFamily::Elasticsearch, &response, &[0, 1], &mut None).unwrap();
        assert_eq!(found[1].code, "ES.ServiceException");
        assert_eq!(
            found[1].message,
            format!("Error received from Elasticsearch cluster. {first}")
        );
    }

    /// As recorded for 403: the status as a string and the raw item error, retried.
    #[test]
    fn other_item_statuses_keep_their_status_and_raw_error() {
        let raw = r#"{"type":"cluster_block_exception","reason":"index [i] blocked by: [FORBIDDEN/8/index write (api)];"}"#;
        let response =
            format!(r#"{{"errors":true,"items":[{{"index":{{"status":403,"error":{raw}}}}}]}}"#);
        let found = item_failures(OS, &response, &[0], &mut None).unwrap();
        assert_eq!(found[0].code, "403");
        assert_eq!(found[0].message, raw);
        assert!(found[0].retriable);
    }

    #[test]
    fn unmatched_bulk_responses_are_errors_not_successes() {
        let item = |status: i64| json!({"index": {"status": status}});
        let code = |response: Value, sent: usize| {
            let positions: Vec<usize> = (0..sent).collect();
            failures(response, &positions).err().unwrap().code
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
        // A success that does not account for the records must not commit them.
        assert_eq!(code(json!({"errors": false}), 1), "OS.ClusterError");
        assert_eq!(
            code(json!({"errors": false, "items": [{}]}), 1),
            "OS.ClusterError"
        );
        assert_eq!(
            code(json!({"errors": false, "items": [{"index": {}}]}), 1),
            "OS.ClusterError"
        );
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
    fn errors_false_still_reports_a_failed_item() {
        let response = json!({"errors": false, "items": [{"index": {"status": 400}}]});
        assert_eq!(failures(response, &[0]).unwrap().len(), 1);
    }

    fn bulk_batch(arrivals: Vec<DateTime<Utc>>) -> (DeliveryBatch, SearchDelivery) {
        let config = json!({
            "IndexName": "activity",
            "ClusterEndpoint": "http://localhost:9200",
            "S3Configuration": {"RoleARN": "r", "BucketARN": "arn:aws:s3:::b"},
        });
        let destination = delivery(OPENSEARCH, &config).unwrap();
        let search = destination.search.clone().unwrap();
        let batch = DeliveryBatch {
            stream: crate::StreamKey {
                account_id: "1".into(),
                region: "us-east-1".into(),
                name: "s".into(),
                instance_id: uuid::Uuid::new_v4(),
            },
            version_id: "1".into(),
            destination,
            records: vec![bytes::Bytes::from_static(b"{}"); arrivals.len()],
            document_ids: Vec::new(),
            oldest_arrival: arrivals[0],
            delivery_time: arrivals[0],
            object_key: "k".into(),
            arrivals,
        };
        (batch, search)
    }

    /// A record's index comes from its own arrival, so where a replay happens to
    /// split batches cannot send its stable `_id` to another index. Recorded on AWS
    /// with two records either side of an hour under OneHour rotation and a 900 s
    /// buffer: they went to the -11 and -12 indexes. The buffer window covered both,
    /// but the recording does not show that they shared one bulk request.
    #[test]
    fn records_of_one_batch_go_to_the_index_of_their_own_arrival() {
        let day = |d: &str| {
            DateTime::parse_from_rfc3339(&format!("{d}T23:59:59Z"))
                .unwrap()
                .with_timezone(&Utc)
        };
        let (batch, search) = bulk_batch(vec![day("2026-02-25"), day("2026-02-26")]);
        let body = String::from_utf8(bulk_body(&batch, &search, &[0, 1])).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert!(lines[0].contains("activity-2026-02-25"), "{}", lines[0]);
        assert!(lines[2].contains("activity-2026-02-26"), "{}", lines[2]);

        let failure = Failure {
            index: 1,
            code: "OS.ServiceException".into(),
            message: String::new(),
            retriable: false,
            bad_request_item: true,
        };
        let line: Value = serde_json::from_str(&failed_record(&batch, &search, &failure)).unwrap();
        assert_eq!(line["osIndexName"], "activity-2026-02-26");
        assert_eq!(
            line["arrivalTimestamp"],
            day("2026-02-26").timestamp() * 1000
        );
    }

    /// What AWS rejects is refused, with its recorded messages.
    #[test]
    fn stored_settings_are_validated_apart_from_delivery_support() {
        let valid = json!({
            "RoleARN": "r",
            "IndexName": "activity",
            "DomainARN": "arn:aws:es:us-east-1:123456789012:domain/d",
            "S3Configuration": {"RoleARN": "r", "BucketARN": "arn:aws:s3:::b"},
        });
        validate_search_destination(&valid).unwrap();
        let without = |member: &str| {
            let mut config = valid.clone();
            config.as_object_mut().unwrap().remove(member);
            config
        };
        for member in ["RoleARN", "IndexName", "DomainARN", "S3Configuration"] {
            assert!(
                validate_search_destination(&without(member)).is_err(),
                "{member}"
            );
        }
        assert_eq!(
            validate_search_destination(&without("DomainARN"))
                .unwrap_err()
                .message(),
            "Provide either the Elasticsearch cluster endpoint or domain arn. Must specify \
             cluster endpoint for cross account Elasticsearch cluster."
        );
        let mut both = valid.clone();
        both["ClusterEndpoint"] = json!("https://x.example.com");
        assert_eq!(
            validate_search_destination(&both).unwrap_err().message(),
            "Cannot provide both Elasticsearch cluster endpoint and domain ARN. Provide either \
             the cluster endpoint or domain arn. Must specify cluster endpoint for cross \
             account elasticsearch cluster."
        );
        let mut expressions = valid.clone();
        expressions["S3Configuration"]["Prefix"] = json!("!{firehose:error-output-type}/");
        assert_eq!(
            validate_search_destination(&expressions)
                .unwrap_err()
                .message(),
            "The supplied prefix(es) do not satisfy the following constraint: ErrorOutputPrefix \
             cannot be null or empty when Prefix contains expressions"
        );
        for (member, value) in [
            ("IndexRotationPeriod", json!("Hourly")),
            ("S3BackupMode", json!("Everything")),
            ("RetryOptions", json!({"DurationInSeconds": 7201})),
            ("BufferingHints", json!({"SizeInMBs": 101})),
            (
                "S3Configuration",
                json!({"RoleARN": "r", "BucketARN": "not-an-arn"}),
            ),
        ] {
            let mut config = valid.clone();
            config[member] = value;
            assert!(validate_search_destination(&config).is_err(), "{member}");
        }
    }

    #[test]
    fn http_status_maps_to_a_family_error_code() {
        for (status, name) in [
            (400, "ServiceException"),
            (403, "AccessDenied"),
            (404, "ResourceNotFound"),
            (500, "ClusterError"),
            (502, "BadGateway"),
            (504, "GatewayTimeout"),
        ] {
            let status = reqwest::StatusCode::from_u16(status).unwrap();
            assert_eq!(status_error(OS, status, "u", "").code, format!("OS.{name}"));
            assert_eq!(
                status_error(SearchFamily::Elasticsearch, status, "u", "").code,
                format!("ES.{name}")
            );
        }
        // The cluster's body is kept as it came, after the family's prefix.
        let body = r#"{"error":{"type":"illegal_argument_exception"},"status":400}"#;
        let error = status_error(OS, reqwest::StatusCode::BAD_REQUEST, "u", body);
        assert_eq!(error.message, OS.service_exception(body));
    }

    /// Recorded: part of a batch indexed with nothing refused before sending, the
    /// 400 items become a parse exception. Inferred from six recordings; see there.
    #[test]
    fn parse_exception_needs_partial_success_and_no_refused_record() {
        let failed = || Failure {
            index: 1,
            code: "OS.ServiceException".into(),
            message: "m".into(),
            retriable: false,
            bad_request_item: true,
        };
        let mut progress = BulkProgress {
            failed: vec![failed()],
            any_indexed: true,
            ..Default::default()
        };
        apply_parse_exception(OS, &mut progress);
        assert_eq!(
            progress.failed[0].code,
            "OS.AmazonOpenSearchServiceParseException"
        );
        assert_eq!(
            progress.failed[0].message,
            "The Amazon OpenSearch Service cluster returned an \
             AmazonOpenSearchServiceParseException. Ensure that the data being put is valid."
        );
        let mut nothing_indexed = BulkProgress {
            failed: vec![failed()],
            ..Default::default()
        };
        apply_parse_exception(OS, &mut nothing_indexed);
        assert_eq!(nothing_indexed.failed[0].code, "OS.ServiceException");
        let mut with_refused = BulkProgress {
            failed: vec![failed()],
            rejected: vec![failed()],
            any_indexed: true,
            ..Default::default()
        };
        apply_parse_exception(OS, &mut with_refused);
        assert_eq!(with_refused.failed[0].code, "OS.ServiceException");
    }

    /// The two recorded refusals of a `ClusterEndpoint`, and one that answers.
    #[test]
    fn cluster_endpoints_are_probed() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        probe_cluster_endpoint(&format!("http://127.0.0.1:{port}")).unwrap();
        drop(listener);
        assert_eq!(
            probe_cluster_endpoint(&format!("http://127.0.0.1:{port}"))
                .unwrap_err()
                .message(),
            "Error connecting to the provided ElasticSearch cluster endpoint.  Ensure that the \
             ElasticSearch cluster endpoint is valid."
        );
        assert_eq!(
            probe_cluster_endpoint("https://no-such-host.invalid")
                .unwrap_err()
                .message(),
            "Invalid Elasticsearch Cluster Endpoint https://no-such-host.invalid. If this \
             Elasticsearch domain is in VPC, please specify VpcConfiguration."
        );
    }

    /// Recorded for `DurationInSeconds` 60: 5 attempts, the last at the deadline.
    #[test]
    fn bulk_retries_double_and_stop_at_the_retry_duration() {
        let attempts = BulkAttempts::default();
        assert_eq!(attempts.retry_delay(Duration::from_secs(60)), None);
        attempts.set_bulk_retry_due(true);
        attempts.begin();
        let delay = attempts.retry_delay(Duration::from_secs(60)).unwrap();
        assert_eq!(delay, FIRST_BULK_RETRY);
        attempts.begin();
        assert_eq!(
            attempts.retry_delay(Duration::from_secs(60)).unwrap(),
            FIRST_BULK_RETRY * 2
        );
        // Whatever is left of a short duration caps the wait.
        let capped = attempts.retry_delay(Duration::from_secs(3)).unwrap();
        assert!(capped <= Duration::from_secs(3), "{capped:?}");
    }

    /// The shapes of the two kinds of failure record, member for member.
    #[test]
    fn failure_records_have_the_recorded_members_in_order() {
        let at = DateTime::parse_from_rfc3339("2026-11-09T11:52:24.700Z")
            .unwrap()
            .with_timezone(&Utc);
        let (mut batch, search) = bulk_batch(vec![at]);
        batch.document_ids = vec!["4968.3".to_owned()];
        let failure = Failure {
            index: 0,
            code: "OS.ServiceException".into(),
            message: "m".into(),
            retriable: false,
            bad_request_item: true,
        };
        let members = |line: &str| -> Vec<String> {
            line.split(",\"")
                .map(|part| {
                    part.trim_start_matches("{\"")
                        .split('"')
                        .next()
                        .unwrap()
                        .to_owned()
                })
                .collect()
        };
        let line = failed_record(&batch, &search, &failure);
        assert_eq!(
            members(&line),
            [
                "attemptsMade",
                "arrivalTimestamp",
                "errorCode",
                "errorMessage",
                "attemptEndingTimestamp",
                "rawData",
                "subsequenceNumber",
                "osDocumentId",
                "osIndexName",
                "osTypeName"
            ]
        );
        let parsed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["subsequenceNumber"], 3);
        assert_eq!(parsed["osDocumentId"], "4968.3");
        assert_eq!(parsed["osTypeName"], Value::Null);
        assert_eq!(parsed["arrivalTimestamp"], at.timestamp() * 1000);

        let rejected = rejected_record(&batch, &failure);
        assert_eq!(members(&rejected).len(), 7);
        let parsed: Value = serde_json::from_str(&rejected).unwrap();
        assert_eq!(parsed["attemptsMade"], 0);
        assert_eq!(parsed["subsequenceNumber"], 0);
    }

    /// Only empty and non-UTF-8 records are refused before sending. Everything
    /// else goes to the cluster untouched, however broken.
    #[test]
    fn only_empty_and_undecodable_records_are_refused_before_sending() {
        let at = Utc::now();
        let records: Vec<&'static [u8]> = vec![
            b"{\"ok\":1}",
            b"{\"a\":1}\n{\"a\":2}",
            b"not json",
            b"   ",
            b"{\"t\":1}\n",
            b"",
            b"\xff\xfe binary",
        ];
        let (mut batch, search) = bulk_batch(vec![at; records.len()]);
        batch.records = records.into_iter().map(bytes::Bytes::from_static).collect();
        let progress = classify_records(&batch);
        assert_eq!(progress.pending, vec![0, 1, 2, 3, 4]);
        let codes: Vec<(usize, &str)> = progress
            .rejected
            .iter()
            .map(|failure| (failure.index, failure.code.as_str()))
            .collect();
        assert_eq!(
            codes,
            [(5, "EmptyRecordException"), (6, "InvalidEncodingException")]
        );
        assert_eq!(
            progress.rejected[0].message,
            "com.amazonaws.firehose.v2.delivery.opensearch.processor.\
             OpenSearchRecordsInMemoryRenderer$EmptyRecordException: The record did not contain \
             any JSON documents"
        );
        assert_eq!(
            progress.rejected[1].message,
            "com.amazonaws.firehose.v2.delivery.opensearch.processing.parser.\
             NoModificationPayloadParser$InvalidEncodingException: The data could not be decoded \
             as UTF-8"
        );

        let body = bulk_body(&batch, &search, &progress.pending);
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains("\n{\"a\":1}\n{\"a\":2}\n"), "{text}");
        assert!(text.contains("\n   \n"), "{text}");
        assert!(text.ends_with("{\"t\":1}\n"), "one newline ends it: {text}");
    }

    #[test]
    fn direct_put_id_prefixes_look_like_sequence_numbers() {
        let prefix = direct_put_id_prefix();
        assert_eq!(prefix.len(), 56);
        assert!(prefix.bytes().all(|b| b.is_ascii_digit()));
        assert_ne!(prefix, direct_put_id_prefix());
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
        assert_eq!(prefix(OPENSEARCH), "p/AmazonOpenSearchService-failed/");
        assert_eq!(prefix(ELASTICSEARCH), "p/elasticsearch-failed/");
        // An unevaluated expression would otherwise become a literal key.
        let search = delivery(
            OPENSEARCH,
            &config("e/!{firehose:error-output-type}/!{timestamp:yyyy}/"),
        )
        .unwrap()
        .search
        .unwrap();
        let at = DateTime::parse_from_rfc3339("2026-11-09T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            search.failure_directory(at),
            "e/AmazonOpenSearchService-failed/2026/"
        );
        let search = delivery(OPENSEARCH, &config("")).unwrap().search.unwrap();
        assert_eq!(
            search.failure_directory(at),
            "p/AmazonOpenSearchService-failed/2026/11/09/14/"
        );
    }

    /// A stream stored with both before they were refused keeps delivering.
    #[test]
    fn a_stored_destination_naming_both_targets_delivers_to_its_cluster() {
        let config = json!({
            "IndexName": "activity",
            "ClusterEndpoint": "http://localhost:9200",
            "DomainARN": "arn:aws:es:us-east-1:123456789012:domain/d",
            "S3Configuration": {"RoleARN": "r", "BucketARN": "arn:aws:s3:::b"},
        });
        assert!(validate_stored(&config).is_err());
        let search = delivery(OPENSEARCH, &config).unwrap().search.unwrap();
        assert_eq!(
            search.target,
            SearchTarget::Cluster("http://localhost:9200".to_owned())
        );
    }

    #[test]
    fn a_domain_arn_destination_targets_its_domain() {
        let arn = "arn:aws:es:us-east-1:123456789012:domain/d";
        let config = json!({
            "IndexName": "activity",
            "DomainARN": arn,
            "S3Configuration": {"RoleARN": "r", "BucketARN": "arn:aws:s3:::b"},
        });
        for kind in [ELASTICSEARCH, OPENSEARCH] {
            let search = delivery(kind, &config).unwrap().search.unwrap();
            assert_eq!(search.target, SearchTarget::Domain(arn.to_owned()));
        }
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
