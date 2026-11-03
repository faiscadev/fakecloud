//! Elasticsearch / OpenSearch Service destinations: records are indexed with
//! the `_bulk` API at `ClusterEndpoint`, and the destination's
//! `S3Configuration` receives failed (or all) documents.

use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Utc};
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
}

/// A document Firehose could not index, with the error that rejected it.
pub(crate) struct Failure {
    pub index: usize,
    pub code: String,
    pub message: String,
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
    delivery.buffering = buffering_policy(Some(size.unwrap_or(5)), Some(interval.unwrap_or(300)))?;
    let failed_folder = if kind == OPENSEARCH {
        "AmazonOpenSearchService-failed/"
    } else {
        "elasticsearch-failed/"
    };
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
    });
    Ok(delivery)
}

pub(crate) fn index_name(base: &str, rotation: &str, at: DateTime<Utc>) -> String {
    let suffix = match rotation {
        "OneHour" => at.format("-%Y-%m-%d-%H").to_string(),
        "OneDay" => at.format("-%Y-%m-%d").to_string(),
        "OneWeek" => at.format("-%Y-w%V").to_string(),
        "OneMonth" => at.format("-%Y-%m").to_string(),
        _ => String::new(),
    };
    format!("{base}{suffix}")
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

pub(crate) fn bulk_body(batch: &DeliveryBatch, search: &SearchDelivery, index: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (position, record) in batch.records.iter().enumerate() {
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

/// Per-document failures in a `_bulk` response, in request order.
pub(crate) fn item_failures(response: &Value) -> Vec<Failure> {
    if response["errors"].as_bool() != Some(true) {
        return Vec::new();
    }
    response["items"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, item)| {
            let result = item.as_object()?.values().next()?;
            let status = result["status"].as_u64().unwrap_or(0);
            (status >= 300).then(|| Failure {
                index,
                code: status.to_string(),
                message: result["error"].to_string(),
            })
        })
        .collect()
}

pub(crate) fn post_bulk(
    search: &SearchDelivery,
    index: &str,
    batch: &DeliveryBatch,
) -> Result<Vec<Failure>, String> {
    let url = format!("{}/_bulk", search.endpoint);
    let body = bulk_body(batch, search, index);
    tokio::runtime::Handle::current().block_on(async {
        let response = reqwest::Client::new()
            .post(&url)
            .header("content-type", "application/json")
            .timeout(Duration::from_secs(60))
            .body(body)
            .send()
            .await
            .map_err(|e| format!("bulk request to {url} failed: {e}"))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("bulk request to {url} returned {status}: {text}"));
        }
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|e| format!("bulk response from {url} is not JSON: {e}"))?;
        Ok(item_failures(&parsed))
    })
}

/// One line of the failed-documents backup object, in Firehose's format.
pub(crate) fn failed_record(
    batch: &DeliveryBatch,
    search: &SearchDelivery,
    index: &str,
    failure: &Failure,
    attempts: u64,
) -> Vec<u8> {
    let arrival = batch
        .arrivals
        .get(failure.index)
        .unwrap_or(&batch.oldest_arrival);
    let mut line = json!({
        "attemptsMade": attempts,
        "arrivalTimestamp": arrival.timestamp_millis(),
        "errorCode": failure.code,
        "errorMessage": failure.message,
        "attemptEndingTimestamp": Utc::now().timestamp_millis(),
        "rawData": base64::engine::general_purpose::STANDARD.encode(&batch.records[failure.index]),
        "subsequenceNumber": 0,
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
    fn item_failures_keep_request_positions() {
        let response = json!({"errors": true, "items": [
            {"index": {"status": 201}},
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
        ]});
        let failures = item_failures(&response);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].index, 1);
        assert_eq!(failures[0].code, "400");
        assert!(failures[0].message.contains("mapper_parsing_exception"));
        assert!(item_failures(&json!({"errors": false, "items": []})).is_empty());
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
}
