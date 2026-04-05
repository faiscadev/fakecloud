use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use md5::Md5;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{HashMap, VecDeque};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{MessageAttribute, RedrivePolicy, SharedSqsState, SqsMessage, SqsQueue};

pub struct SqsService {
    state: SharedSqsState,
}

impl SqsService {
    pub fn new(state: SharedSqsState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for SqsService {
    fn service_name(&self) -> &str {
        "sqs"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateQueue" => self.create_queue(&req),
            "DeleteQueue" => self.delete_queue(&req),
            "ListQueues" => self.list_queues(&req),
            "GetQueueUrl" => self.get_queue_url(&req),
            "GetQueueAttributes" => self.get_queue_attributes(&req),
            "SetQueueAttributes" => self.set_queue_attributes(&req),
            "SendMessage" => self.send_message(&req),
            "SendMessageBatch" => self.send_message_batch(&req),
            "ReceiveMessage" => self.receive_message(&req).await,
            "DeleteMessage" => self.delete_message(&req),
            "DeleteMessageBatch" => self.delete_message_batch(&req),
            "PurgeQueue" => self.purge_queue(&req),
            "ChangeMessageVisibility" => self.change_message_visibility(&req),
            "ChangeMessageVisibilityBatch" => self.change_message_visibility_batch(&req),
            "ListQueueTags" => self.list_queue_tags(&req),
            "TagQueue" => self.tag_queue(&req),
            "UntagQueue" => self.untag_queue(&req),
            "AddPermission" => self.add_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            "ListDeadLetterSourceQueues" => self.list_dead_letter_source_queues(&req),
            _ => Err(AwsServiceError::action_not_implemented("sqs", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateQueue",
            "DeleteQueue",
            "ListQueues",
            "GetQueueUrl",
            "GetQueueAttributes",
            "SetQueueAttributes",
            "SendMessage",
            "SendMessageBatch",
            "ReceiveMessage",
            "DeleteMessage",
            "DeleteMessageBatch",
            "PurgeQueue",
            "ChangeMessageVisibility",
            "ChangeMessageVisibilityBatch",
            "ListQueueTags",
            "TagQueue",
            "UntagQueue",
            "AddPermission",
            "RemovePermission",
            "ListDeadLetterSourceQueues",
        ]
    }
}

/// Parse the request body. SQS supports both JSON protocol (modern SDKs like aws-sdk-rust)
/// and Query protocol (boto3, older SDKs). For Query protocol, params are in query_params.
fn parse_body(req: &AwsRequest) -> Value {
    // Try JSON first
    if let Ok(v) = serde_json::from_slice::<Value>(&req.body) {
        if v.is_object() && !v.as_object().unwrap().is_empty() {
            return v;
        }
    }
    // Fall back to query params (Query protocol / form-encoded)
    if !req.query_params.is_empty() {
        let mut map = serde_json::Map::new();
        for (k, v) in &req.query_params {
            map.insert(k.clone(), Value::String(v.clone()));
        }
        // Handle nested Attribute.N.Name/Value patterns
        let mut attrs = serde_json::Map::new();
        for i in 1..=20 {
            let name_key = format!("Attribute.{i}.Name");
            let value_key = format!("Attribute.{i}.Value");
            if let (Some(name), Some(value)) = (
                req.query_params.get(&name_key),
                req.query_params.get(&value_key),
            ) {
                attrs.insert(name.clone(), Value::String(value.clone()));
            }
        }
        if !attrs.is_empty() {
            map.insert("Attributes".to_string(), Value::Object(attrs));
        }
        // Handle batch entry patterns: *Entry.N.Field or *.N.Field
        // e.g. SendMessageBatchRequestEntry.1.Id=foo&SendMessageBatchRequestEntry.1.MessageBody=bar
        // Also: DeleteMessageBatchRequestEntry.1.Id=...&DeleteMessageBatchRequestEntry.1.ReceiptHandle=...
        // Also: ChangeMessageVisibilityBatchRequestEntry.1.Id=...
        let entries = parse_batch_entries(&req.query_params);
        if !entries.is_empty() {
            map.insert("Entries".to_string(), Value::Array(entries));
        }
        return Value::Object(map);
    }
    Value::Object(Default::default())
}

/// Parse batch entry parameters like `SendMessageBatchRequestEntry.1.Id=foo`.
/// Returns a Vec of JSON objects, one per entry index.
fn parse_batch_entries(params: &HashMap<String, String>) -> Vec<Value> {
    use std::collections::BTreeMap;

    // Find all entry-like keys: anything matching *.N.Field pattern
    let mut entries_map: BTreeMap<u32, serde_json::Map<String, Value>> = BTreeMap::new();

    for (key, value) in params {
        // Match patterns like "SomethingEntry.N.Field" or "Entries.member.N.Field"
        let parts: Vec<&str> = key.split('.').collect();
        if parts.len() >= 3 {
            // Try to find the numeric index
            for (i, part) in parts.iter().enumerate() {
                if let Ok(idx) = part.parse::<u32>() {
                    // Everything after the index is the field name
                    let field = parts[i + 1..].join(".");
                    if !field.is_empty() {
                        entries_map
                            .entry(idx)
                            .or_default()
                            .insert(field, Value::String(value.clone()));
                    }
                    break;
                }
            }
        }
    }

    entries_map.into_values().map(Value::Object).collect()
}

/// Extract an i64 from a Value that might be a number or a string (Query protocol sends strings).
fn val_as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn json_response(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_wrap(action: &str, inner: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\
         <{action}Result>{inner}</{action}Result>\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    )
}

fn xml_metadata_only(action: &str, request_id: &str) -> AwsResponse {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    );
    AwsResponse::xml(StatusCode::OK, xml)
}

fn sqs_response(action: &str, body: Value, request_id: &str, is_query: bool) -> AwsResponse {
    if !is_query {
        return json_response(body);
    }
    match action {
        "CreateQueue" => {
            let url = body["QueueUrl"].as_str().unwrap_or("");
            let inner = format!("<QueueUrl>{}</QueueUrl>", xml_escape(url));
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "GetQueueUrl" => {
            let url = body["QueueUrl"].as_str().unwrap_or("");
            let inner = format!("<QueueUrl>{}</QueueUrl>", xml_escape(url));
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "ListQueues" => {
            let mut inner = String::new();
            if let Some(urls) = body["QueueUrls"].as_array() {
                for url in urls {
                    if let Some(u) = url.as_str() {
                        inner.push_str(&format!("<QueueUrl>{}</QueueUrl>", xml_escape(u)));
                    }
                }
            }
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "SendMessage" => {
            let msg_id = body["MessageId"].as_str().unwrap_or("");
            let md5 = body["MD5OfMessageBody"].as_str().unwrap_or("");
            let inner = format!(
                "<MessageId>{}</MessageId><MD5OfMessageBody>{}</MD5OfMessageBody>",
                xml_escape(msg_id),
                xml_escape(md5)
            );
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "ReceiveMessage" => {
            let mut inner = String::new();
            if let Some(messages) = body["Messages"].as_array() {
                for msg in messages {
                    inner.push_str("<Message>");
                    if let Some(id) = msg["MessageId"].as_str() {
                        inner.push_str(&format!("<MessageId>{}</MessageId>", xml_escape(id)));
                    }
                    if let Some(rh) = msg["ReceiptHandle"].as_str() {
                        inner.push_str(&format!(
                            "<ReceiptHandle>{}</ReceiptHandle>",
                            xml_escape(rh)
                        ));
                    }
                    if let Some(md5) = msg["MD5OfBody"].as_str() {
                        inner.push_str(&format!("<MD5OfBody>{}</MD5OfBody>", xml_escape(md5)));
                    }
                    if let Some(body_str) = msg["Body"].as_str() {
                        inner.push_str(&format!("<Body>{}</Body>", xml_escape(body_str)));
                    }
                    if let Some(attrs) = msg["Attributes"].as_object() {
                        for (k, v) in attrs {
                            if let Some(val) = v.as_str() {
                                inner.push_str(&format!(
                                    "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
                                    xml_escape(k),
                                    xml_escape(val)
                                ));
                            }
                        }
                    }
                    if let Some(msg_attrs) = msg["MessageAttributes"].as_object() {
                        for (name, attr) in msg_attrs {
                            inner.push_str("<MessageAttribute>");
                            inner.push_str(&format!("<Name>{}</Name>", xml_escape(name)));
                            inner.push_str("<Value>");
                            if let Some(dt) = attr["DataType"].as_str() {
                                inner.push_str(&format!("<DataType>{}</DataType>", xml_escape(dt)));
                            }
                            if let Some(sv) = attr["StringValue"].as_str() {
                                inner.push_str(&format!(
                                    "<StringValue>{}</StringValue>",
                                    xml_escape(sv)
                                ));
                            }
                            inner.push_str("</Value>");
                            inner.push_str("</MessageAttribute>");
                        }
                    }
                    inner.push_str("</Message>");
                }
            }
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "GetQueueAttributes" => {
            let mut inner = String::new();
            if let Some(attrs) = body["Attributes"].as_object() {
                for (k, v) in attrs {
                    let val = v.as_str().unwrap_or("");
                    inner.push_str(&format!(
                        "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
                        xml_escape(k),
                        xml_escape(val)
                    ));
                }
            }
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "SendMessageBatch" => {
            let mut inner = String::new();
            if let Some(successful) = body["Successful"].as_array() {
                for entry in successful {
                    inner.push_str("<SendMessageBatchResultEntry>");
                    if let Some(id) = entry["Id"].as_str() {
                        inner.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
                    }
                    if let Some(msg_id) = entry["MessageId"].as_str() {
                        inner.push_str(&format!("<MessageId>{}</MessageId>", xml_escape(msg_id)));
                    }
                    if let Some(md5) = entry["MD5OfMessageBody"].as_str() {
                        inner.push_str(&format!(
                            "<MD5OfMessageBody>{}</MD5OfMessageBody>",
                            xml_escape(md5)
                        ));
                    }
                    inner.push_str("</SendMessageBatchResultEntry>");
                }
            }
            if let Some(failed) = body["Failed"].as_array() {
                for entry in failed {
                    inner.push_str("<BatchResultErrorEntry>");
                    if let Some(id) = entry["Id"].as_str() {
                        inner.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
                    }
                    if let Some(code) = entry["Code"].as_str() {
                        inner.push_str(&format!("<Code>{}</Code>", xml_escape(code)));
                    }
                    if let Some(msg) = entry["Message"].as_str() {
                        inner.push_str(&format!("<Message>{}</Message>", xml_escape(msg)));
                    }
                    if let Some(sf) = entry["SenderFault"].as_bool() {
                        inner.push_str(&format!("<SenderFault>{sf}</SenderFault>"));
                    }
                    inner.push_str("</BatchResultErrorEntry>");
                }
            }
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "DeleteMessageBatch" => {
            let mut inner = String::new();
            if let Some(successful) = body["Successful"].as_array() {
                for entry in successful {
                    inner.push_str("<DeleteMessageBatchResultEntry>");
                    if let Some(id) = entry["Id"].as_str() {
                        inner.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
                    }
                    inner.push_str("</DeleteMessageBatchResultEntry>");
                }
            }
            if let Some(failed) = body["Failed"].as_array() {
                for entry in failed {
                    inner.push_str("<BatchResultErrorEntry>");
                    if let Some(id) = entry["Id"].as_str() {
                        inner.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
                    }
                    if let Some(code) = entry["Code"].as_str() {
                        inner.push_str(&format!("<Code>{}</Code>", xml_escape(code)));
                    }
                    if let Some(msg) = entry["Message"].as_str() {
                        inner.push_str(&format!("<Message>{}</Message>", xml_escape(msg)));
                    }
                    if let Some(sf) = entry["SenderFault"].as_bool() {
                        inner.push_str(&format!("<SenderFault>{sf}</SenderFault>"));
                    }
                    inner.push_str("</BatchResultErrorEntry>");
                }
            }
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        "ChangeMessageVisibilityBatch" => {
            let mut inner = String::new();
            if let Some(successful) = body["Successful"].as_array() {
                for entry in successful {
                    inner.push_str("<ChangeMessageVisibilityBatchResultEntry>");
                    if let Some(id) = entry["Id"].as_str() {
                        inner.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
                    }
                    inner.push_str("</ChangeMessageVisibilityBatchResultEntry>");
                }
            }
            if let Some(failed) = body["Failed"].as_array() {
                for entry in failed {
                    inner.push_str("<BatchResultErrorEntry>");
                    if let Some(id) = entry["Id"].as_str() {
                        inner.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
                    }
                    if let Some(code) = entry["Code"].as_str() {
                        inner.push_str(&format!("<Code>{}</Code>", xml_escape(code)));
                    }
                    if let Some(msg) = entry["Message"].as_str() {
                        inner.push_str(&format!("<Message>{}</Message>", xml_escape(msg)));
                    }
                    if let Some(sf) = entry["SenderFault"].as_bool() {
                        inner.push_str(&format!("<SenderFault>{sf}</SenderFault>"));
                    }
                    inner.push_str("</BatchResultErrorEntry>");
                }
            }
            AwsResponse::xml(StatusCode::OK, xml_wrap(action, &inner, request_id))
        }
        // DeleteQueue, DeleteMessage, PurgeQueue, SetQueueAttributes, ChangeMessageVisibility
        _ => xml_metadata_only(action, request_id),
    }
}

impl SqsService {
    fn create_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_name = body["QueueName"]
            .as_str()
            .ok_or_else(|| missing_param("QueueName"))?
            .to_string();

        let is_fifo = queue_name.ends_with(".fifo");

        // Validate FIFO queue attributes
        if let Some(attrs) = body["Attributes"].as_object() {
            if let Some(fifo_val) = attrs.get("FifoQueue").and_then(|v| v.as_str()) {
                if fifo_val == "true" && !is_fifo {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        "The queue name must end with the .fifo suffix for a FIFO queue.",
                    ));
                }
            }
        }

        // Validate queue name
        let base_name = if is_fifo {
            queue_name.trim_end_matches(".fifo")
        } else {
            &queue_name
        };
        if !is_valid_queue_name(base_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
            ));
        }

        let mut new_attributes = HashMap::new();
        if let Some(attrs) = body["Attributes"].as_object() {
            for (k, v) in attrs {
                if let Some(s) = v.as_str() {
                    new_attributes.insert(k.clone(), s.to_string());
                }
            }
        }

        let mut state = self.state.write();

        if let Some(url) = state.name_to_url.get(&queue_name) {
            // Queue exists - check if attributes match
            if let Some(existing) = state.queues.get(url) {
                // If caller passed attributes, check for conflicts
                if !new_attributes.is_empty() {
                    for (k, v) in &new_attributes {
                        if let Some(existing_val) = existing.attributes.get(k.trim()) {
                            // Normalize JSON values for comparison (e.g. RedrivePolicy)
                            let val_matches = if let (Ok(a), Ok(b)) = (
                                serde_json::from_str::<Value>(existing_val),
                                serde_json::from_str::<Value>(v),
                            ) {
                                a == b
                            } else {
                                existing_val == v
                            };
                            if !val_matches {
                                return Err(AwsServiceError::aws_error(
                                    StatusCode::BAD_REQUEST,
                                    "QueueAlreadyExists",
                                    "A queue already exists with the same name and a different value for attribute VisibilityTimeout.",
                                ));
                            }
                        }
                    }
                }
            }
            return Ok(sqs_response(
                "CreateQueue",
                json!({ "QueueUrl": url }),
                &req.request_id,
                req.is_query_protocol,
            ));
        }

        let queue_url = format!("{}/{}/{}", state.endpoint, state.account_id, queue_name);

        let mut attributes = HashMap::new();
        // Default attributes
        attributes.insert("VisibilityTimeout".to_string(), "30".to_string());
        attributes.insert("DelaySeconds".to_string(), "0".to_string());
        attributes.insert("MaximumMessageSize".to_string(), "1048576".to_string());
        attributes.insert("MessageRetentionPeriod".to_string(), "345600".to_string());
        attributes.insert("ReceiveMessageWaitTimeSeconds".to_string(), "0".to_string());
        if is_fifo {
            attributes.insert("FifoQueue".to_string(), "true".to_string());
            attributes.insert("ContentBasedDeduplication".to_string(), "false".to_string());
            attributes.insert("DeduplicationScope".to_string(), "queue".to_string());
            attributes.insert("FifoThroughputLimit".to_string(), "perQueue".to_string());
        }

        // Validate MaximumMessageSize before inserting
        if let Some(mms) = new_attributes.get("MaximumMessageSize") {
            if let Ok(size) = mms.parse::<u64>() {
                if !(1024..=1_048_576).contains(&size) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidAttributeValue",
                        "Invalid value for the parameter MaximumMessageSize.",
                    ));
                }
            }
        }

        // Override with provided attributes (trim keys to handle trailing whitespace)
        for (k, v) in new_attributes {
            attributes.insert(k.trim().to_string(), v);
        }

        let redrive_policy = attributes.get("RedrivePolicy").and_then(|rp_str| {
            let rp: Value = serde_json::from_str(rp_str).ok()?;
            let dead_letter_target_arn = rp["deadLetterTargetArn"].as_str()?.to_string();
            let max_receive_count = rp["maxReceiveCount"]
                .as_u64()
                .or_else(|| rp["maxReceiveCount"].as_str()?.parse().ok())?
                as u32;
            Some(RedrivePolicy {
                dead_letter_target_arn,
                max_receive_count,
            })
        });

        // Validate that the DLQ actually exists
        if let Some(ref rp) = redrive_policy {
            let dlq_exists = state
                .queues
                .values()
                .any(|q| q.arn == rp.dead_letter_target_arn);
            if !dlq_exists {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AWS.SimpleQueueService.NonExistentQueue",
                    format!(
                        "Dead letter target does not exist: {}",
                        rp.dead_letter_target_arn
                    ),
                ));
            }
            // Validate FIFO queue can only use FIFO DLQ
            if is_fifo {
                let dlq_is_fifo = state
                    .queues
                    .values()
                    .find(|q| q.arn == rp.dead_letter_target_arn)
                    .map(|q| q.is_fifo)
                    .unwrap_or(false);
                if !dlq_is_fifo {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        "Dead-letter queue must be the same type of queue as the source.",
                    ));
                }
            }
        }

        // Normalize RedrivePolicy JSON (convert maxReceiveCount to integer)
        if let Some(ref rp) = redrive_policy {
            // Format like Python json.dumps: {"key": value, "key": value}
            attributes.insert(
                "RedrivePolicy".to_string(),
                format!(
                    "{{\"deadLetterTargetArn\": \"{}\", \"maxReceiveCount\": {}}}",
                    rp.dead_letter_target_arn, rp.max_receive_count
                ),
            );
        }

        // Parse tags
        let mut tags = HashMap::new();
        if let Some(tags_obj) = body["tags"].as_object() {
            for (k, v) in tags_obj {
                if let Some(s) = v.as_str() {
                    tags.insert(k.clone(), s.to_string());
                }
            }
        }
        // Also check Tags (JSON protocol)
        if let Some(tags_obj) = body["Tags"].as_object() {
            for (k, v) in tags_obj {
                if let Some(s) = v.as_str() {
                    tags.insert(k.clone(), s.to_string());
                }
            }
        }

        let now = Utc::now();
        let created_ts = now.timestamp();

        attributes.insert("CreatedTimestamp".to_string(), created_ts.to_string());
        attributes.insert("LastModifiedTimestamp".to_string(), created_ts.to_string());

        let queue = SqsQueue {
            arn: format!(
                "arn:aws:sqs:{}:{}:{}",
                state.region, state.account_id, queue_name
            ),
            queue_name: queue_name.clone(),
            queue_url: queue_url.clone(),
            created_at: now,
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: HashMap::new(),
            redrive_policy,
            tags,
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: HashMap::new(),
        };

        state.name_to_url.insert(queue_name, queue_url.clone());
        state.queues.insert(queue_url.clone(), queue);

        Ok(sqs_response(
            "CreateQueue",
            json!({ "QueueUrl": queue_url }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn delete_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(&queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .remove(&resolved_url)
            .ok_or_else(queue_not_found)?;
        state.name_to_url.remove(&queue.queue_name);

        Ok(sqs_response(
            "DeleteQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn list_queues(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let prefix = body["QueueNamePrefix"].as_str();
        let state = self.state.read();

        let urls: Vec<String> = state
            .queues
            .values()
            .filter(|q| prefix.map(|p| q.queue_name.starts_with(p)).unwrap_or(true))
            .map(|q| q.queue_url.clone())
            .collect();

        Ok(sqs_response(
            "ListQueues",
            json!({ "QueueUrls": urls }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn get_queue_url(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_name = body["QueueName"]
            .as_str()
            .ok_or_else(|| missing_param("QueueName"))?;

        let state = self.state.read();
        let url = state
            .name_to_url
            .get(queue_name)
            .ok_or_else(queue_not_found)?;

        Ok(sqs_response(
            "GetQueueUrl",
            json!({ "QueueUrl": url }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn get_queue_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let state = self.state.read();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Check what attributes were requested
        let requested_names = body["AttributeNames"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>());

        // If no AttributeNames specified, return empty (per AWS behavior for JSON protocol)
        if requested_names.is_none() || requested_names.as_ref().map(|n| n.is_empty()) == Some(true)
        {
            // For query protocol, some clients don't pass AttributeNames and expect empty
            return Ok(sqs_response(
                "GetQueueAttributes",
                json!({}),
                &req.request_id,
                req.is_query_protocol,
            ));
        }

        let names = requested_names.unwrap();
        let want_all = names.contains(&"All");

        // Validate attribute names
        let valid_attrs = [
            "All",
            "Policy",
            "VisibilityTimeout",
            "MaximumMessageSize",
            "MessageRetentionPeriod",
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "CreatedTimestamp",
            "LastModifiedTimestamp",
            "QueueArn",
            "ApproximateNumberOfMessagesDelayed",
            "DelaySeconds",
            "ReceiveMessageWaitTimeSeconds",
            "RedrivePolicy",
            "FifoQueue",
            "ContentBasedDeduplication",
            "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds",
            "DeduplicationScope",
            "FifoThroughputLimit",
            "RedriveAllowPolicy",
            "SqsManagedSseEnabled",
        ];
        for name in &names {
            if !valid_attrs.contains(name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidAttributeName",
                    format!("Unknown Attribute {name}."),
                ));
            }
        }

        let now = Utc::now();
        let mut attrs = queue.attributes.clone();
        attrs.insert("QueueArn".to_string(), queue.arn.clone());

        // Count visible messages (not delayed)
        let visible_count = queue
            .messages
            .iter()
            .filter(|m| m.visible_at.map(|v| v <= now).unwrap_or(true))
            .count();
        // Count expired inflight as visible
        let expired_inflight = queue
            .inflight
            .iter()
            .filter(|m| m.visible_at.map(|v| v <= now).unwrap_or(false))
            .count();
        let still_inflight = queue.inflight.len() - expired_inflight;
        attrs.insert(
            "ApproximateNumberOfMessages".to_string(),
            (visible_count + expired_inflight).to_string(),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesNotVisible".to_string(),
            still_inflight.to_string(),
        );
        // Count delayed messages
        let delayed_count = queue
            .messages
            .iter()
            .filter(|m| m.visible_at.map(|v| v > now).unwrap_or(false))
            .count();
        attrs.insert(
            "ApproximateNumberOfMessagesDelayed".to_string(),
            delayed_count.to_string(),
        );

        if !want_all {
            attrs.retain(|k, _| names.contains(&k.as_str()));
        }

        if attrs.is_empty() {
            Ok(sqs_response(
                "GetQueueAttributes",
                json!({}),
                &req.request_id,
                req.is_query_protocol,
            ))
        } else {
            Ok(sqs_response(
                "GetQueueAttributes",
                json!({ "Attributes": attrs }),
                &req.request_id,
                req.is_query_protocol,
            ))
        }
    }

    fn send_message(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();
        let message_body = body["MessageBody"]
            .as_str()
            .ok_or_else(|| missing_param("MessageBody"))?
            .to_string();

        let message_group_id = body["MessageGroupId"].as_str().map(|s| s.to_string());
        let message_dedup_id = body["MessageDeduplicationId"]
            .as_str()
            .map(|s| s.to_string());

        let message_attributes = parse_message_attributes(&body);

        // Validate message attributes
        validate_message_attributes(&message_attributes)?;

        // Parse MessageSystemAttributes (e.g., AWSTraceHeader)
        let system_attributes = parse_message_system_attributes(&body);

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(&queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // FIFO delay validation - FIFO queues don't support per-message delays
        if queue.is_fifo {
            let delay = val_as_i64(&body["DelaySeconds"]).unwrap_or(0);
            if delay != 0 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {} for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.", delay),
                ));
            }
        }

        // FIFO validations
        if queue.is_fifo {
            if message_group_id.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MissingParameter",
                    "The request must contain the parameter MessageGroupId.",
                ));
            }
            if message_dedup_id.is_none()
                && queue
                    .attributes
                    .get("ContentBasedDeduplication")
                    .map(|v| v.as_str())
                    != Some("true")
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
        }

        // FIFO dedup - use content-based dedup if no explicit ID
        let effective_dedup_id = if queue.is_fifo {
            message_dedup_id.clone().or_else(|| {
                if queue
                    .attributes
                    .get("ContentBasedDeduplication")
                    .map(|v| v.as_str())
                    == Some("true")
                {
                    // Use SHA-256 of message body as dedup ID
                    Some(sha256_hex(&message_body))
                } else {
                    None
                }
            })
        } else {
            None
        };

        if queue.is_fifo {
            if let Some(ref dedup_id) = effective_dedup_id {
                let now = Utc::now();
                queue.dedup_cache.retain(|_, expiry| *expiry > now);
                if queue.dedup_cache.contains_key(dedup_id) {
                    let msg_id = uuid::Uuid::new_v4().to_string();
                    let seq = queue.next_sequence_number;
                    queue.next_sequence_number += 1;
                    let mut resp = json!({
                        "MessageId": msg_id,
                        "MD5OfMessageBody": md5_hex(&message_body),
                        "SequenceNumber": seq.to_string(),
                    });
                    if !message_attributes.is_empty() {
                        resp["MD5OfMessageAttributes"] =
                            json!(md5_of_message_attributes(&message_attributes));
                    }
                    return Ok(sqs_response(
                        "SendMessage",
                        resp,
                        &req.request_id,
                        req.is_query_protocol,
                    ));
                }
                queue
                    .dedup_cache
                    .insert(dedup_id.clone(), now + chrono::Duration::minutes(5));
            }
        }

        // MaximumMessageSize validation
        let max_message_size: usize = queue
            .attributes
            .get("MaximumMessageSize")
            .and_then(|s| s.parse().ok())
            .unwrap_or(262144);
        if message_body.len() > max_message_size {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "One or more parameters are invalid. Reason: Message must be shorter than {} bytes.",
                    max_message_size
                ),
            ));
        }

        // Validate delay seconds (max 900 = 15 minutes)
        if let Some(d) = val_as_i64(&body["DelaySeconds"]) {
            if d > 900 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {d} for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900, if provided."),
                ));
            }
        }

        let delay: i64 = val_as_i64(&body["DelaySeconds"])
            .or_else(|| {
                queue
                    .attributes
                    .get("DelaySeconds")
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0);
        let now = Utc::now();
        let visible_at = if delay > 0 {
            Some(now + chrono::Duration::seconds(delay))
        } else {
            None
        };

        let sequence_number = if queue.is_fifo {
            let seq = queue.next_sequence_number;
            queue.next_sequence_number += 1;
            Some(seq.to_string())
        } else {
            None
        };

        let md5_of_attrs = if message_attributes.is_empty() {
            None
        } else {
            Some(md5_of_message_attributes(&message_attributes))
        };

        let msg = SqsMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            receipt_handle: None,
            md5_of_body: md5_hex(&message_body),
            body: message_body,
            sent_timestamp: now.timestamp_millis(),
            attributes: system_attributes,
            message_attributes,
            visible_at,
            receive_count: 0,
            message_group_id,
            message_dedup_id: effective_dedup_id,
            created_at: now,
            sequence_number: sequence_number.clone(),
        };

        let mut resp = json!({
            "MessageId": msg.message_id,
            "MD5OfMessageBody": msg.md5_of_body,
        });
        if let Some(seq) = &sequence_number {
            resp["SequenceNumber"] = json!(seq);
        }
        if let Some(md5) = &md5_of_attrs {
            resp["MD5OfMessageAttributes"] = json!(md5);
        }
        queue.messages.push_back(msg);

        Ok(sqs_response(
            "SendMessage",
            resp,
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    async fn receive_message(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url_input = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        // Resolve the queue URL (might be a name)
        let queue_url = {
            let state = self.state.read();
            resolve_queue_url(&queue_url_input, &state).ok_or_else(queue_not_found)?
        };

        let max_messages_raw = val_as_i64(&body["MaxNumberOfMessages"]);
        if let Some(max) = max_messages_raw {
            if !(1..=10).contains(&max) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {max} for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."),
                ));
            }
        }
        let max_messages = max_messages_raw.unwrap_or(1).min(10) as usize;

        let visibility_timeout = val_as_i64(&body["VisibilityTimeout"]);

        let wait_time_raw = val_as_i64(&body["WaitTimeSeconds"]);
        if let Some(wt) = wait_time_raw {
            if !(0..=20).contains(&wt) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value {wt} for parameter WaitTimeSeconds is invalid. Reason: Must be between 0 and 20, if provided."),
                ));
            }
        }
        let wait_time_seconds = wait_time_raw.unwrap_or(0).clamp(0, 20) as u64;

        // Parse requested system attributes
        let attribute_names: Option<Vec<String>> = body["AttributeNames"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });
        // Also check MessageSystemAttributeNames (newer SDK field)
        let sys_attr_names: Option<Vec<String>> =
            body["MessageSystemAttributeNames"].as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });
        let requested_sys_attrs = sys_attr_names.or(attribute_names);

        // Parse requested message attributes filter
        let msg_attr_names: Option<Vec<String>> =
            body["MessageAttributeNames"].as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });

        let request_id = req.request_id.clone();
        let is_query = req.is_query_protocol;

        let deadline = if wait_time_seconds > 0 {
            Some(tokio::time::Instant::now() + std::time::Duration::from_secs(wait_time_seconds))
        } else {
            None
        };

        loop {
            let result = self.try_receive_messages(&queue_url, max_messages, visibility_timeout)?;

            if !result.is_empty() || deadline.is_none() {
                return Ok(format_receive_response(
                    &result,
                    &request_id,
                    is_query,
                    requested_sys_attrs.as_deref(),
                    msg_attr_names.as_deref(),
                ));
            }

            let deadline = deadline.unwrap();
            if tokio::time::Instant::now() >= deadline {
                return Ok(format_receive_response(
                    &result,
                    &request_id,
                    is_query,
                    requested_sys_attrs.as_deref(),
                    msg_attr_names.as_deref(),
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    fn try_receive_messages(
        &self,
        queue_url: &str,
        max_messages: usize,
        req_visibility_timeout: Option<i64>,
    ) -> Result<Vec<SqsMessage>, AwsServiceError> {
        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let visibility_timeout: i64 = req_visibility_timeout
            .or_else(|| {
                queue
                    .attributes
                    .get("VisibilityTimeout")
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(30);

        let is_fifo = queue.is_fifo;
        let now = Utc::now();

        // MessageRetentionPeriod expiry: remove messages older than the retention period
        let retention_seconds: i64 = queue
            .attributes
            .get("MessageRetentionPeriod")
            .and_then(|s| s.parse().ok())
            .unwrap_or(345600); // default 4 days
        queue
            .messages
            .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);
        queue
            .inflight
            .retain(|m| (now - m.created_at).num_seconds() < retention_seconds);

        // Return expired inflight messages
        let mut returned = Vec::new();
        queue.inflight.retain(|m| {
            if let Some(visible_at) = m.visible_at {
                if visible_at <= now {
                    returned.push(m.clone());
                    return false;
                }
            }
            true
        });
        // For FIFO queues, push returned messages to the FRONT to maintain order
        if is_fifo {
            for mut m in returned.into_iter().rev() {
                m.visible_at = None;
                queue.messages.push_front(m);
            }
        } else {
            for mut m in returned {
                m.visible_at = None;
                queue.messages.push_back(m);
            }
        }

        let redrive_policy = queue.redrive_policy.clone();

        let mut received = Vec::new();
        let mut dlq_messages = Vec::new();

        if is_fifo {
            // FIFO: deliver messages in order, respecting group locking.
            // Groups that already have inflight messages are skipped.
            // Multiple messages from the same group CAN be delivered in one batch.
            let mut remaining = VecDeque::new();

            // Build set of groups that have pre-existing inflight messages
            let inflight_groups: std::collections::HashSet<String> = queue
                .inflight
                .iter()
                .filter_map(|m| m.message_group_id.clone())
                .collect();

            while let Some(mut msg) = queue.messages.pop_front() {
                if let Some(visible_at) = msg.visible_at {
                    if visible_at > now {
                        remaining.push_back(msg);
                        continue;
                    }
                }

                let group = msg.message_group_id.as_deref().unwrap_or("").to_string();

                // Skip groups that already have inflight messages from previous receives
                if inflight_groups.contains(&group) {
                    remaining.push_back(msg);
                    continue;
                }

                if received.len() < max_messages {
                    msg.receive_count += 1;
                    if let Some(ref rp) = redrive_policy {
                        if msg.receive_count > rp.max_receive_count {
                            dlq_messages.push((rp.dead_letter_target_arn.clone(), msg));
                            continue;
                        }
                    }
                    let new_handle = uuid::Uuid::new_v4().to_string();
                    queue
                        .receipt_handle_map
                        .entry(msg.message_id.clone())
                        .or_default()
                        .push(new_handle.clone());
                    msg.receipt_handle = Some(new_handle);
                    msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                    received.push(msg);
                } else {
                    remaining.push_back(msg);
                    break;
                }
            }

            while let Some(m) = queue.messages.pop_front() {
                remaining.push_back(m);
            }
            queue.messages = remaining;
        } else {
            // Standard queue with Fair Queues support:
            // When messages have MessageGroupId, prioritize groups with fewer
            // in-flight messages to prevent noisy neighbor starvation.

            // Count in-flight messages per group
            let mut inflight_per_group: HashMap<String, usize> = HashMap::new();
            for m in &queue.inflight {
                if let Some(ref group) = m.message_group_id {
                    *inflight_per_group.entry(group.clone()).or_default() += 1;
                }
            }

            // Collect all visible messages
            let mut visible: Vec<SqsMessage> = Vec::new();
            let mut remaining = VecDeque::new();
            while let Some(msg) = queue.messages.pop_front() {
                if let Some(visible_at) = msg.visible_at {
                    if visible_at > now {
                        remaining.push_back(msg);
                        continue;
                    }
                }
                visible.push(msg);
            }

            // Sort by fairness: messages from groups with fewer in-flight messages come first.
            // Messages without a group ID are treated as having 0 in-flight (highest priority).
            visible.sort_by_key(|m| {
                m.message_group_id
                    .as_ref()
                    .and_then(|g| inflight_per_group.get(g).copied())
                    .unwrap_or(0)
            });

            // Pick up to max_messages from the sorted list
            for mut msg in visible {
                if received.len() < max_messages {
                    msg.receive_count += 1;
                    if let Some(ref rp) = redrive_policy {
                        if msg.receive_count > rp.max_receive_count {
                            dlq_messages.push((rp.dead_letter_target_arn.clone(), msg));
                            continue;
                        }
                    }
                    let new_handle = uuid::Uuid::new_v4().to_string();
                    queue
                        .receipt_handle_map
                        .entry(msg.message_id.clone())
                        .or_default()
                        .push(new_handle.clone());
                    msg.receipt_handle = Some(new_handle);
                    msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                    received.push(msg);
                } else {
                    remaining.push_back(msg);
                }
            }

            queue.messages = remaining;
        }

        for msg in &received {
            queue.inflight.push(msg.clone());
        }

        // Move messages to DLQ
        for (dlq_arn, mut msg) in dlq_messages {
            if let Some(dlq) = state.queues.values_mut().find(|q| q.arn == dlq_arn) {
                msg.receipt_handle = None;
                msg.visible_at = None;
                dlq.messages.push_back(msg);
            }
        }

        Ok(received)
    }

    fn delete_message(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let receipt_handle = body["ReceiptHandle"]
            .as_str()
            .ok_or_else(|| missing_param("ReceiptHandle"))?;

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Find the message_id associated with this receipt handle
        let message_id = find_message_id_for_receipt(queue, receipt_handle);

        if let Some(msg_id) = message_id {
            // Delete by message_id (any receipt handle for this message works)
            // Keep the receipt_handle_map entry so subsequent deletes are idempotent
            queue.inflight.retain(|m| m.message_id != msg_id);
            queue.messages.retain(|m| m.message_id != msg_id);
        } else {
            // Receipt handle not found - error
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReceiptHandleIsInvalid",
                "The input receipt handle is invalid.",
            ));
        }

        Ok(sqs_response(
            "DeleteMessage",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn purge_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        queue.messages.clear();
        queue.inflight.clear();

        Ok(sqs_response(
            "PurgeQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn change_message_visibility(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let receipt_handle = body["ReceiptHandle"]
            .as_str()
            .ok_or_else(|| missing_param("ReceiptHandle"))?;
        let visibility_timeout = val_as_i64(&body["VisibilityTimeout"])
            .ok_or_else(|| missing_param("VisibilityTimeout"))?;

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();

        // Find the message_id associated with this receipt handle
        let message_id = find_message_id_for_receipt(queue, receipt_handle);

        if let Some(msg_id) = message_id {
            let new_visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
            let mut found = false;

            // Check inflight messages
            for msg in &mut queue.inflight {
                if msg.message_id == msg_id {
                    msg.visible_at = new_visible_at;
                    found = true;
                    break;
                }
            }

            // Also check messages queue (message may have become visible again)
            if !found {
                for msg in &mut queue.messages {
                    if msg.message_id == msg_id {
                        msg.visible_at = new_visible_at;
                        found = true;
                        break;
                    }
                }
            }

            if !found {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ReceiptHandleIsInvalid",
                    "The input receipt handle is invalid.",
                ));
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReceiptHandleIsInvalid",
                "The input receipt handle is invalid.",
            ));
        }

        Ok(sqs_response(
            "ChangeMessageVisibility",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn change_message_visibility_batch(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing_param("Entries"))?
            .clone();

        let mut state = self.state.write();
        let queue = state
            .queues
            .get_mut(queue_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();
        let mut successful = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = match entry["Id"].as_str() {
                Some(id) => id.to_string(),
                None => continue,
            };
            let receipt_handle = match entry["ReceiptHandle"].as_str() {
                Some(rh) => rh,
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "ReceiptHandle is required",
                    }));
                    continue;
                }
            };
            let visibility_timeout = match val_as_i64(&entry["VisibilityTimeout"]) {
                Some(vt) => vt,
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "VisibilityTimeout is required",
                    }));
                    continue;
                }
            };

            let message_id = find_message_id_for_receipt(queue, receipt_handle);

            if let Some(msg_id) = message_id {
                let new_visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                let mut found = false;

                for msg in &mut queue.inflight {
                    if msg.message_id == msg_id {
                        msg.visible_at = new_visible_at;
                        found = true;
                        break;
                    }
                }

                if !found {
                    for msg in &mut queue.messages {
                        if msg.message_id == msg_id {
                            msg.visible_at = new_visible_at;
                            found = true;
                            break;
                        }
                    }
                }

                if found {
                    successful.push(json!({ "Id": id }));
                } else {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "ReceiptHandleIsInvalid",
                        "Message": "The input receipt handle is invalid.",
                    }));
                }
            } else {
                failed.push(json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The input receipt handle is invalid.",
                }));
            }
        }

        Ok(sqs_response(
            "ChangeMessageVisibilityBatch",
            json!({
                "Successful": successful,
                "Failed": failed,
            }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn set_queue_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        if let Some(attrs) = body["Attributes"].as_object() {
            for (k, v) in attrs {
                if let Some(s) = v.as_str() {
                    // Setting an empty value for Policy or RedrivePolicy removes it
                    if s.is_empty()
                        && (k == "Policy" || k == "RedrivePolicy" || k == "RedriveAllowPolicy")
                    {
                        queue.attributes.remove(k);
                        if k == "RedrivePolicy" {
                            queue.redrive_policy = None;
                        }
                    } else {
                        queue.attributes.insert(k.clone(), s.to_string());
                    }
                }
            }

            // Update redrive_policy if set
            if let Some(rp_str) = attrs.get("RedrivePolicy").and_then(|v| v.as_str()) {
                if !rp_str.is_empty() {
                    if let Ok(rp) = serde_json::from_str::<Value>(rp_str) {
                        let dead_letter_target_arn = rp["deadLetterTargetArn"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                        let max_receive_count = rp["maxReceiveCount"]
                            .as_u64()
                            .or_else(|| rp["maxReceiveCount"].as_str()?.parse().ok())
                            .unwrap_or(0) as u32;
                        if !dead_letter_target_arn.is_empty() && max_receive_count > 0 {
                            queue.redrive_policy = Some(RedrivePolicy {
                                dead_letter_target_arn: dead_letter_target_arn.clone(),
                                max_receive_count,
                            });
                            // Normalize the stored JSON (Python json.dumps format)
                            queue.attributes.insert(
                                "RedrivePolicy".to_string(),
                                format!(
                                    "{{\"deadLetterTargetArn\": \"{}\", \"maxReceiveCount\": {}}}",
                                    dead_letter_target_arn, max_receive_count
                                ),
                            );
                        }
                    }
                }
            }
        }

        Ok(sqs_response(
            "SetQueueAttributes",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn send_message_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing_param("Entries"))?
            .clone();

        // Validate batch is not empty
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                "There should be at least one SendMessageBatchRequestEntry in the request.",
            ));
        }

        // Max 10 entries
        if entries.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
                format!(
                    "Maximum number of entries per request are 10. You have sent {}.",
                    entries.len()
                ),
            ));
        }

        // Validate entry IDs and check for duplicates
        let mut seen_ids: Vec<String> = Vec::new();
        for entry in &entries {
            if let Some(id) = entry["Id"].as_str() {
                // Validate ID format
                if !is_valid_batch_id(id) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.InvalidBatchEntryId",
                        "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
                    ));
                }
                if seen_ids.contains(&id.to_string()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                        format!("Id {} repeated.", id),
                    ));
                }
                seen_ids.push(id.to_string());
            }
        }

        // Validate total batch size
        let total_size: usize = entries
            .iter()
            .filter_map(|e| e["MessageBody"].as_str())
            .map(|b| b.len())
            .sum();
        if total_size > 1_048_576 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.BatchRequestTooLong",
                format!(
                    "Batch requests cannot be longer than 1048576 bytes. You have sent {} bytes.",
                    total_size
                ),
            ));
        }

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(&queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();
        let mut successful = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        let is_fifo = queue.is_fifo;
        let content_based_dedup = queue
            .attributes
            .get("ContentBasedDeduplication")
            .map(|v| v.as_str())
            == Some("true");
        let max_message_size: usize = queue
            .attributes
            .get("MaximumMessageSize")
            .and_then(|s| s.parse().ok())
            .unwrap_or(262144);
        let queue_delay: Option<i64> = queue
            .attributes
            .get("DelaySeconds")
            .and_then(|s| s.parse().ok());

        for entry in &entries {
            let id = match entry["Id"].as_str() {
                Some(id) => id.to_string(),
                None => continue,
            };
            let message_body = match entry["MessageBody"].as_str() {
                Some(b) => b.to_string(),
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "MessageBody is required",
                    }));
                    continue;
                }
            };

            // MaximumMessageSize validation
            if message_body.len() > max_message_size {
                failed.push(json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "InvalidParameterValue",
                    "Message": format!(
                        "One or more parameters are invalid. Reason: Message must be shorter than {} bytes.",
                        max_message_size
                    ),
                }));
                continue;
            }

            // Per-entry delay validation (max 900 seconds)
            if let Some(d) = val_as_i64(&entry["DelaySeconds"]) {
                if d > 900 {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "InvalidParameterValue",
                        "Message": format!("Value {} for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900, if provided.", d),
                    }));
                    continue;
                }
            }

            let message_group_id = entry["MessageGroupId"].as_str().map(|s| s.to_string());
            let message_dedup_id = entry["MessageDeduplicationId"]
                .as_str()
                .map(|s| s.to_string());

            // FIFO validations
            if is_fifo {
                if message_group_id.is_none() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MissingParameter",
                        "The request must contain the parameter MessageGroupId.",
                    ));
                }
                if message_dedup_id.is_none() && !content_based_dedup {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                    ));
                }
            }

            let delay: i64 = val_as_i64(&entry["DelaySeconds"])
                .or(queue_delay)
                .unwrap_or(0);
            let visible_at = if delay > 0 {
                Some(now + chrono::Duration::seconds(delay))
            } else {
                None
            };

            let message_attributes = parse_message_attributes(entry);

            let sequence_number = if is_fifo {
                let seq = queue.next_sequence_number;
                queue.next_sequence_number += 1;
                Some(seq.to_string())
            } else {
                None
            };

            let md5_of_attrs = if message_attributes.is_empty() {
                None
            } else {
                Some(md5_of_message_attributes(&message_attributes))
            };

            let msg = SqsMessage {
                message_id: uuid::Uuid::new_v4().to_string(),
                receipt_handle: None,
                md5_of_body: md5_hex(&message_body),
                body: message_body,
                sent_timestamp: now.timestamp_millis(),
                attributes: HashMap::new(),
                message_attributes,
                visible_at,
                receive_count: 0,
                message_group_id,
                message_dedup_id,
                created_at: now,
                sequence_number: sequence_number.clone(),
            };

            let mut entry_resp = json!({
                "Id": id,
                "MessageId": msg.message_id,
                "MD5OfMessageBody": msg.md5_of_body,
            });
            if let Some(seq) = &sequence_number {
                entry_resp["SequenceNumber"] = json!(seq);
            }
            if let Some(md5) = &md5_of_attrs {
                entry_resp["MD5OfMessageAttributes"] = json!(md5);
            }
            successful.push(entry_resp);
            queue.messages.push_back(msg);
        }

        Ok(sqs_response(
            "SendMessageBatch",
            json!({
                "Successful": successful,
                "Failed": failed,
            }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn delete_message_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing_param("Entries"))?
            .clone();

        // Validate batch is not empty
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                "There should be at least one DeleteMessageBatchRequestEntry in the request.",
            ));
        }

        // Check for duplicate IDs
        let mut seen_ids = std::collections::HashSet::new();
        for entry in &entries {
            if let Some(id) = entry["Id"].as_str() {
                if !seen_ids.insert(id.to_string()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                        "Two or more batch entries in the operation have the same Id.",
                    ));
                }
            }
        }

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        let mut successful = Vec::new();
        let mut failed: Vec<Value> = Vec::new();

        for entry in &entries {
            let id = match entry["Id"].as_str() {
                Some(id) => id.to_string(),
                None => continue,
            };
            let receipt_handle = match entry["ReceiptHandle"].as_str() {
                Some(rh) => rh,
                None => {
                    failed.push(json!({
                        "Id": id,
                        "SenderFault": true,
                        "Code": "MissingParameter",
                        "Message": "ReceiptHandle is required",
                    }));
                    continue;
                }
            };

            let message_id = find_message_id_for_receipt(queue, receipt_handle);

            if let Some(msg_id) = message_id {
                queue.inflight.retain(|m| m.message_id != msg_id);
                queue.messages.retain(|m| m.message_id != msg_id);
                successful.push(json!({ "Id": id }));
            } else {
                failed.push(json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": format!(
                        "The input receipt handle \"{}\" is not a valid receipt handle.",
                        receipt_handle
                    ),
                }));
            }
        }

        Ok(sqs_response(
            "DeleteMessageBatch",
            json!({
                "Successful": successful,
                "Failed": failed,
            }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn list_queue_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let state = self.state.read();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get(&resolved_url)
            .ok_or_else(queue_not_found)?;
        let tags = &queue.tags;

        Ok(sqs_response(
            "ListQueueTags",
            json!({ "Tags": tags }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn tag_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let tags = body["Tags"].as_object();

        // Validate tags are not empty
        if tags.is_none() || tags.map(|t| t.is_empty()) == Some(true) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Tags.",
            ));
        }

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        if let Some(tags_obj) = tags {
            // Check total tag count after adding
            let mut merged = queue.tags.clone();
            for (k, v) in tags_obj {
                if let Some(s) = v.as_str() {
                    merged.insert(k.clone(), s.to_string());
                }
            }
            if merged.len() > 50 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Too many tags added for queue {}.", queue.queue_name),
                ));
            }
            queue.tags = merged;
        }

        Ok(sqs_response(
            "TagQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn untag_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let tag_keys = body["TagKeys"].as_array();

        // Validate tag keys are not empty
        if tag_keys.is_none() || tag_keys.map(|t| t.is_empty()) == Some(true) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Tag keys must be between 1 and 128 characters in length.",
            ));
        }

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        if let Some(keys) = tag_keys {
            for k in keys {
                if let Some(s) = k.as_str() {
                    queue.tags.remove(s);
                }
            }
        }

        Ok(sqs_response(
            "UntagQueue",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn add_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let label = body["Label"]
            .as_str()
            .ok_or_else(|| missing_param("Label"))?;

        // Parse Actions - may come as array or query params
        let actions: Vec<String> = if let Some(arr) = body["Actions"].as_array() {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            parse_numbered_params(&body, "ActionName")
        };

        // Parse AWSAccountIds
        let account_ids: Vec<String> = if let Some(arr) = body["AWSAccountIds"].as_array() {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            let mut ids = Vec::new();
            if let Some(obj) = body.as_object() {
                for i in 1..=20 {
                    let key = format!("AWSAccountId.{i}");
                    if let Some(v) = obj.get(&key).and_then(|v| v.as_str()) {
                        ids.push(v.to_string());
                    }
                }
            }
            ids
        };

        // Validate actions not empty
        if actions.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Actions.",
            ));
        }

        // Validate account IDs not empty
        if account_ids.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Value [] for parameter PrincipalId is invalid. Reason: Unable to verify.",
            ));
        }

        // Validate max 7 actions
        if actions.len() > 7 {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "OverLimit",
                format!(
                    "{} Actions were found, maximum allowed is 7.",
                    actions.len()
                ),
            ));
        }

        // Validate no owner-only actions
        let owner_only = [
            "AddPermission",
            "RemovePermission",
            "CreateQueue",
            "DeleteQueue",
            "SetQueueAttributes",
            "TagQueue",
            "UntagQueue",
        ];
        for action in &actions {
            if owner_only.contains(&action.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Value SQS:{action} for parameter ActionName is invalid. Reason: Only the queue owner is allowed to invoke this action."
                    ),
                ));
            }
        }

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Check for duplicate label
        if queue.permission_labels.contains(&label.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Value {label} for parameter Label is invalid. Reason: Already exists."),
            ));
        }

        queue.permission_labels.push(label.to_string());

        // Build policy
        let mut statements: Vec<Value> = Vec::new();

        // Load existing policy
        if let Some(policy_str) = queue.attributes.get("Policy") {
            if let Ok(policy) = serde_json::from_str::<Value>(policy_str) {
                if let Some(stmts) = policy["Statement"].as_array() {
                    statements = stmts.clone();
                }
            }
        }

        // Add new statement for each account/action pair
        for account_id in &account_ids {
            let action_values: Vec<String> = actions
                .iter()
                .map(|a| {
                    if a == "*" {
                        "SQS:*".to_string()
                    } else {
                        format!("SQS:{a}")
                    }
                })
                .collect();

            let action_value = if action_values.len() == 1 {
                json!(action_values[0])
            } else {
                json!(action_values)
            };

            statements.push(json!({
                "Sid": label,
                "Effect": "Allow",
                "Principal": {
                    "AWS": format!("arn:aws:iam::{account_id}:root")
                },
                "Action": action_value,
                "Resource": queue.arn,
            }));
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Id": format!("{}/SQSDefaultPolicy", queue.arn),
            "Statement": statements,
        });

        queue.attributes.insert(
            "Policy".to_string(),
            serde_json::to_string(&policy).unwrap(),
        );

        Ok(sqs_response(
            "AddPermission",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn remove_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let label = body["Label"]
            .as_str()
            .ok_or_else(|| missing_param("Label"))?;

        let mut state = self.state.write();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Check label exists
        if !queue.permission_labels.contains(&label.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Value {label} for parameter Label is invalid. Reason: can't find label on existing policy."
                ),
            ));
        }

        queue.permission_labels.retain(|l| l != label);

        // Remove from policy
        if let Some(policy_str) = queue.attributes.get("Policy").cloned() {
            if let Ok(mut policy) = serde_json::from_str::<Value>(&policy_str) {
                if let Some(stmts) = policy["Statement"].as_array() {
                    let filtered: Vec<Value> = stmts
                        .iter()
                        .filter(|s| s["Sid"].as_str() != Some(label))
                        .cloned()
                        .collect();
                    policy["Statement"] = json!(filtered);
                    queue.attributes.insert(
                        "Policy".to_string(),
                        serde_json::to_string(&policy).unwrap(),
                    );
                }
            }
        }

        Ok(sqs_response(
            "RemovePermission",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    fn list_dead_letter_source_queues(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let state = self.state.read();
        let resolved_url = resolve_queue_url(queue_url, &state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get(&resolved_url)
            .ok_or_else(queue_not_found)?;
        let queue_arn = queue.arn.clone();

        // Find all queues whose redrive policy targets this queue
        let source_urls: Vec<String> = state
            .queues
            .values()
            .filter(|q| {
                q.redrive_policy
                    .as_ref()
                    .map(|rp| rp.dead_letter_target_arn == queue_arn)
                    .unwrap_or(false)
            })
            .map(|q| q.queue_url.clone())
            .collect();

        Ok(sqs_response(
            "ListDeadLetterSourceQueues",
            json!({ "queueUrls": source_urls }),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}

fn format_receive_response(
    received: &[SqsMessage],
    request_id: &str,
    is_query: bool,
    requested_sys_attrs: Option<&[String]>,
    msg_attr_names: Option<&[String]>,
) -> AwsResponse {
    let now_millis = Utc::now().timestamp_millis();

    let messages: Vec<Value> = received
        .iter()
        .map(|m| {
            let mut msg_json = json!({
                "MessageId": m.message_id,
                "ReceiptHandle": m.receipt_handle,
                "MD5OfBody": m.md5_of_body,
                "Body": m.body,
            });

            // Only include system attributes if requested
            if let Some(names) = requested_sys_attrs {
                if !names.is_empty() {
                    let want_all = names.iter().any(|n| n == "All");
                    let mut sys_attrs = serde_json::Map::new();

                    if want_all || names.iter().any(|n| n == "ApproximateReceiveCount") {
                        sys_attrs.insert(
                            "ApproximateReceiveCount".to_string(),
                            json!(m.receive_count.to_string()),
                        );
                    }
                    if want_all || names.iter().any(|n| n == "SentTimestamp") {
                        sys_attrs.insert(
                            "SentTimestamp".to_string(),
                            json!(m.sent_timestamp.to_string()),
                        );
                    }
                    if want_all
                        || names
                            .iter()
                            .any(|n| n == "ApproximateFirstReceiveTimestamp")
                    {
                        sys_attrs.insert(
                            "ApproximateFirstReceiveTimestamp".to_string(),
                            json!(now_millis.to_string()),
                        );
                    }
                    if want_all || names.iter().any(|n| n == "SenderId") {
                        sys_attrs.insert("SenderId".to_string(), json!("AIDAIT2UOQQY3AUEKVGXU"));
                    }
                    if want_all || names.iter().any(|n| n == "MessageGroupId") {
                        if let Some(ref group_id) = m.message_group_id {
                            sys_attrs.insert("MessageGroupId".to_string(), json!(group_id));
                        }
                    }
                    if want_all || names.iter().any(|n| n == "MessageDeduplicationId") {
                        if let Some(ref dedup_id) = m.message_dedup_id {
                            sys_attrs.insert("MessageDeduplicationId".to_string(), json!(dedup_id));
                        }
                    }
                    if want_all || names.iter().any(|n| n == "SequenceNumber") {
                        if let Some(ref seq) = m.sequence_number {
                            sys_attrs.insert("SequenceNumber".to_string(), json!(seq));
                        }
                    }
                    if want_all || names.iter().any(|n| n == "AWSTraceHeader") {
                        // Include AWSTraceHeader if message has it in system attributes
                        if let Some(trace) = m.attributes.get("AWSTraceHeader") {
                            sys_attrs.insert("AWSTraceHeader".to_string(), json!(trace));
                        }
                    }

                    if !sys_attrs.is_empty() {
                        msg_json["Attributes"] = Value::Object(sys_attrs);
                    }
                }
            }

            // Filter message attributes
            let filtered_attrs: HashMap<String, &MessageAttribute> =
                if let Some(names) = msg_attr_names {
                    if names.is_empty() {
                        HashMap::new()
                    } else if names.iter().any(|n| n == "All" || n == ".*") {
                        m.message_attributes
                            .iter()
                            .map(|(k, v)| (k.clone(), v))
                            .collect()
                    } else {
                        m.message_attributes
                            .iter()
                            .filter(|(k, _)| {
                                names.iter().any(|n| {
                                    if n.ends_with(".*") {
                                        k.starts_with(n.trim_end_matches(".*"))
                                    } else {
                                        k.as_str() == n.as_str()
                                    }
                                })
                            })
                            .map(|(k, v)| (k.clone(), v))
                            .collect()
                    }
                } else {
                    HashMap::new()
                };

            if !filtered_attrs.is_empty() {
                let attrs: serde_json::Map<String, Value> = filtered_attrs
                    .iter()
                    .map(|(k, v)| {
                        let mut attr = json!({ "DataType": v.data_type });
                        if let Some(ref sv) = v.string_value {
                            attr["StringValue"] = json!(sv);
                        }
                        if let Some(ref bv) = v.binary_value {
                            use base64::Engine;
                            attr["BinaryValue"] =
                                json!(base64::engine::general_purpose::STANDARD.encode(bv));
                        }
                        (k.clone(), attr)
                    })
                    .collect();
                msg_json["MessageAttributes"] = Value::Object(attrs);
                msg_json["MD5OfMessageAttributes"] =
                    json!(md5_of_message_attributes_from_refs(&filtered_attrs));
            }

            msg_json
        })
        .collect();

    let body = if messages.is_empty() && !is_query {
        // For JSON protocol, omit Messages key when empty
        json!({})
    } else {
        json!({ "Messages": messages })
    };

    sqs_response("ReceiveMessage", body, request_id, is_query)
}

fn parse_message_attributes(body: &Value) -> HashMap<String, MessageAttribute> {
    let mut result = HashMap::new();
    if let Some(attrs) = body["MessageAttributes"].as_object() {
        for (name, val) in attrs {
            let data_type = val["DataType"].as_str().unwrap_or("String").to_string();
            let string_value = val["StringValue"].as_str().map(|s| s.to_string());
            let binary_value = val["BinaryValue"].as_str().and_then(|s| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.decode(s).ok()
            });
            result.insert(
                name.clone(),
                MessageAttribute {
                    data_type,
                    string_value,
                    binary_value,
                },
            );
        }
    }

    // Handle Query protocol MessageAttribute.N.Name/Value patterns
    if let Some(body_obj) = body.as_object() {
        for i in 1..=20 {
            let name_key = format!("MessageAttribute.{i}.Name");
            let type_key = format!("MessageAttribute.{i}.Value.DataType");
            let str_key = format!("MessageAttribute.{i}.Value.StringValue");
            let bin_key = format!("MessageAttribute.{i}.Value.BinaryValue");

            if let Some(name) = body_obj.get(&name_key).and_then(|v| v.as_str()) {
                let data_type = body_obj
                    .get(&type_key)
                    .and_then(|v| v.as_str())
                    .unwrap_or("String")
                    .to_string();
                let string_value = body_obj
                    .get(&str_key)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let binary_value = body_obj
                    .get(&bin_key)
                    .and_then(|v| v.as_str())
                    .and_then(|s| {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.decode(s).ok()
                    });
                result.insert(
                    name.to_string(),
                    MessageAttribute {
                        data_type,
                        string_value,
                        binary_value,
                    },
                );
            }
        }
    }

    result
}

/// Validate message attribute names and data types
fn validate_message_attributes(
    attrs: &HashMap<String, MessageAttribute>,
) -> Result<(), AwsServiceError> {
    for (name, attr) in attrs {
        // Validate attribute name
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "The message attribute name '{}' is invalid. Attribute name can contain A-Z, a-z, 0-9, underscore (_), hyphen (-), and period (.) characters.",
                    name
                ),
            ));
        }

        // Validate data type
        let dt = &attr.data_type;
        let base_type = dt.split('.').next().unwrap_or(dt);
        let valid_prefixes = ["String", "Number", "Binary"];
        if !valid_prefixes.contains(&base_type) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "The message attribute '{name}' has an invalid message attribute type, the set of supported type prefixes is Binary, Number, and String."
                ),
            ));
        }
    }
    Ok(())
}

fn is_valid_queue_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 80 {
        return false;
    }
    name.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

/// Compute MD5 of message attributes per AWS specification.
/// AWS sorts attributes by name, then for each: encode name length (4 bytes BE),
/// name bytes, data type length (4 bytes BE), data type bytes,
/// then transport type (1=String, 2=Binary) and value length + value bytes.
fn md5_of_message_attributes(attrs: &HashMap<String, MessageAttribute>) -> String {
    use md5::Digest;
    let mut sorted: Vec<(&String, &MessageAttribute)> = attrs.iter().collect();
    sorted.sort_by_key(|(k, _)| k.as_str());

    let mut hasher = Md5::new();
    for (name, attr) in sorted {
        // Name
        hasher.update((name.len() as u32).to_be_bytes());
        hasher.update(name.as_bytes());
        // Data type
        hasher.update((attr.data_type.len() as u32).to_be_bytes());
        hasher.update(attr.data_type.as_bytes());

        // Transport type and value
        if attr.data_type.starts_with("String") || attr.data_type.starts_with("Number") {
            hasher.update([1u8]); // STRING transport type
            if let Some(ref sv) = attr.string_value {
                hasher.update((sv.len() as u32).to_be_bytes());
                hasher.update(sv.as_bytes());
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        } else if attr.data_type.starts_with("Binary") {
            hasher.update([2u8]); // BINARY transport type
            if let Some(ref bv) = attr.binary_value {
                hasher.update((bv.len() as u32).to_be_bytes());
                hasher.update(bv);
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        }
    }
    format!("{:032x}", hasher.finalize())
}

/// Same as md5_of_message_attributes but works with borrowed references
fn md5_of_message_attributes_from_refs(attrs: &HashMap<String, &MessageAttribute>) -> String {
    use md5::Digest;
    let mut sorted: Vec<(&String, &&MessageAttribute)> = attrs.iter().collect();
    sorted.sort_by_key(|(k, _)| k.as_str());

    let mut hasher = Md5::new();
    for (name, attr) in sorted {
        hasher.update((name.len() as u32).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update((attr.data_type.len() as u32).to_be_bytes());
        hasher.update(attr.data_type.as_bytes());

        if attr.data_type.starts_with("String") || attr.data_type.starts_with("Number") {
            hasher.update([1u8]);
            if let Some(ref sv) = attr.string_value {
                hasher.update((sv.len() as u32).to_be_bytes());
                hasher.update(sv.as_bytes());
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        } else if attr.data_type.starts_with("Binary") {
            hasher.update([2u8]);
            if let Some(ref bv) = attr.binary_value {
                hasher.update((bv.len() as u32).to_be_bytes());
                hasher.update(bv);
            } else {
                hasher.update(0u32.to_be_bytes());
            }
        }
    }
    format!("{:032x}", hasher.finalize())
}

/// Resolve a QueueUrl that might be a queue name, a path, or a full URL
fn resolve_queue_url(input: &str, state: &crate::state::SqsState) -> Option<String> {
    // Direct match
    if state.queues.contains_key(input) {
        return Some(input.to_string());
    }
    // Try as queue name
    if let Some(url) = state.name_to_url.get(input) {
        return Some(url.clone());
    }
    // Try extracting queue name from URL path (e.g., /123456789012/my-queue)
    let name = input.rsplit('/').next().unwrap_or("");
    if let Some(url) = state.name_to_url.get(name) {
        return Some(url.clone());
    }
    None
}

fn missing_param(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MissingParameter",
        format!("The request must contain the parameter {name}"),
    )
}

fn queue_not_found() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "AWS.SimpleQueueService.NonExistentQueue",
        "The specified queue does not exist.",
    )
}

pub fn md5_hex(input: &str) -> String {
    use md5::Digest;
    let mut hasher = Md5::new();
    hasher.update(input.as_bytes());
    format!("{:032x}", hasher.finalize())
}

pub fn sha256_hex(input: &str) -> String {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:064x}", hasher.finalize())
}

/// Find the message_id associated with a receipt handle by checking both the
/// receipt_handle_map (historical handles) and current message receipt handles.
fn find_message_id_for_receipt(
    queue: &crate::state::SqsQueue,
    receipt_handle: &str,
) -> Option<String> {
    // Check the receipt handle map for any historical handle
    for (msg_id, handles) in &queue.receipt_handle_map {
        if handles.iter().any(|h| h == receipt_handle) {
            return Some(msg_id.clone());
        }
    }
    // Also check current messages/inflight directly
    for msg in &queue.inflight {
        if msg.receipt_handle.as_deref() == Some(receipt_handle) {
            return Some(msg.message_id.clone());
        }
    }
    for msg in &queue.messages {
        if msg.receipt_handle.as_deref() == Some(receipt_handle) {
            return Some(msg.message_id.clone());
        }
    }
    None
}

/// Parse MessageSystemAttributes (e.g., AWSTraceHeader) from the request body.
fn parse_message_system_attributes(body: &Value) -> HashMap<String, String> {
    let mut result = HashMap::new();

    // JSON protocol
    if let Some(attrs) = body["MessageSystemAttributes"].as_object() {
        for (name, val) in attrs {
            if let Some(sv) = val["StringValue"].as_str() {
                result.insert(name.clone(), sv.to_string());
            }
        }
    }

    // Query protocol: MessageSystemAttribute.N.Name/Value
    if let Some(body_obj) = body.as_object() {
        for i in 1..=20 {
            let name_key = format!("MessageSystemAttribute.{i}.Name");
            let str_key = format!("MessageSystemAttribute.{i}.Value.StringValue");

            if let Some(name) = body_obj.get(&name_key).and_then(|v| v.as_str()) {
                if let Some(value) = body_obj.get(&str_key).and_then(|v| v.as_str()) {
                    result.insert(name.to_string(), value.to_string());
                }
            }
        }
    }

    result
}

fn is_valid_batch_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 80 {
        return false;
    }
    id.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

fn parse_numbered_params(body: &Value, prefix: &str) -> Vec<String> {
    let mut result = Vec::new();
    if let Some(obj) = body.as_object() {
        for i in 1..=20 {
            let key = format!("{prefix}.{i}");
            if let Some(v) = obj.get(&key).and_then(|v| v.as_str()) {
                result.push(v.to_string());
            }
        }
    }
    result
}
