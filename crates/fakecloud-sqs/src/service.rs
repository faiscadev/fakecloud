use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{SharedSqsState, SqsMessage, SqsQueue};

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
            "SendMessage" => self.send_message(&req),
            "ReceiveMessage" => self.receive_message(&req),
            "DeleteMessage" => self.delete_message(&req),
            "PurgeQueue" => self.purge_queue(&req),
            "ChangeMessageVisibility" => self.change_message_visibility(&req),
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
            "SendMessage",
            "ReceiveMessage",
            "DeleteMessage",
            "PurgeQueue",
            "ChangeMessageVisibility",
        ]
    }
}

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Object(Default::default()))
}

fn json_response(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

impl SqsService {
    fn create_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_name = body["QueueName"]
            .as_str()
            .ok_or_else(|| missing_param("QueueName"))?
            .to_string();

        let mut state = self.state.write();

        if let Some(url) = state.name_to_url.get(&queue_name) {
            return Ok(json_response(json!({ "QueueUrl": url })));
        }

        let is_fifo = queue_name.ends_with(".fifo");
        let queue_url = format!("http://localhost:4566/{}/{}", state.account_id, queue_name);

        let mut attributes = HashMap::new();
        attributes.insert("VisibilityTimeout".to_string(), "30".to_string());
        if is_fifo {
            attributes.insert("FifoQueue".to_string(), "true".to_string());
        }

        if let Some(attrs) = body["Attributes"].as_object() {
            for (k, v) in attrs {
                if let Some(s) = v.as_str() {
                    attributes.insert(k.clone(), s.to_string());
                }
            }
        }

        let queue = SqsQueue {
            arn: format!(
                "arn:aws:sqs:{}:{}:{}",
                state.region, state.account_id, queue_name
            ),
            queue_name: queue_name.clone(),
            queue_url: queue_url.clone(),
            created_at: Utc::now(),
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: HashMap::new(),
        };

        state.name_to_url.insert(queue_name, queue_url.clone());
        state.queues.insert(queue_url.clone(), queue);

        Ok(json_response(json!({ "QueueUrl": queue_url })))
    }

    fn delete_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();

        let mut state = self.state.write();
        let queue = state
            .queues
            .remove(&queue_url)
            .ok_or_else(queue_not_found)?;
        state.name_to_url.remove(&queue.queue_name);

        Ok(json_response(json!({})))
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

        Ok(json_response(json!({ "QueueUrls": urls })))
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

        Ok(json_response(json!({ "QueueUrl": url })))
    }

    fn get_queue_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let state = self.state.read();
        let queue = state.queues.get(queue_url).ok_or_else(queue_not_found)?;

        let mut attrs = queue.attributes.clone();
        attrs.insert("QueueArn".to_string(), queue.arn.clone());
        attrs.insert(
            "ApproximateNumberOfMessages".to_string(),
            queue.messages.len().to_string(),
        );
        attrs.insert(
            "ApproximateNumberOfMessagesNotVisible".to_string(),
            queue.inflight.len().to_string(),
        );

        Ok(json_response(json!({ "Attributes": attrs })))
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

        let mut state = self.state.write();
        let queue = state
            .queues
            .get_mut(&queue_url)
            .ok_or_else(queue_not_found)?;

        // FIFO dedup
        if queue.is_fifo {
            if let Some(ref dedup_id) = message_dedup_id {
                let now = Utc::now();
                queue.dedup_cache.retain(|_, expiry| *expiry > now);
                if queue.dedup_cache.contains_key(dedup_id) {
                    let msg_id = uuid::Uuid::new_v4().to_string();
                    return Ok(json_response(json!({
                        "MessageId": msg_id,
                        "MD5OfMessageBody": md5_hex(&message_body),
                    })));
                }
                queue
                    .dedup_cache
                    .insert(dedup_id.clone(), now + chrono::Duration::minutes(5));
            }
        }

        let delay: i64 = body["DelaySeconds"].as_i64().unwrap_or(0);
        let now = Utc::now();
        let visible_at = if delay > 0 {
            Some(now + chrono::Duration::seconds(delay))
        } else {
            None
        };

        let msg = SqsMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            receipt_handle: None,
            md5_of_body: md5_hex(&message_body),
            body: message_body,
            sent_timestamp: now.timestamp_millis(),
            attributes: HashMap::new(),
            visible_at,
            receive_count: 0,
            message_group_id,
            message_dedup_id,
        };

        let resp = json!({
            "MessageId": msg.message_id,
            "MD5OfMessageBody": msg.md5_of_body,
        });
        queue.messages.push_back(msg);

        Ok(json_response(resp))
    }

    fn receive_message(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?
            .to_string();
        let max_messages = body["MaxNumberOfMessages"].as_i64().unwrap_or(1).min(10) as usize;

        let mut state = self.state.write();
        let queue = state
            .queues
            .get_mut(&queue_url)
            .ok_or_else(queue_not_found)?;

        let visibility_timeout: i64 = body["VisibilityTimeout"]
            .as_i64()
            .or_else(|| {
                queue
                    .attributes
                    .get("VisibilityTimeout")
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(30);

        let now = Utc::now();

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
        for mut m in returned {
            m.visible_at = None;
            m.receipt_handle = None;
            queue.messages.push_back(m);
        }

        let mut received = Vec::new();
        let mut remaining = VecDeque::new();

        while let Some(mut msg) = queue.messages.pop_front() {
            if let Some(visible_at) = msg.visible_at {
                if visible_at > now {
                    remaining.push_back(msg);
                    continue;
                }
            }

            if received.len() < max_messages {
                msg.receipt_handle = Some(uuid::Uuid::new_v4().to_string());
                msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                msg.receive_count += 1;
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

        for msg in &received {
            queue.inflight.push(msg.clone());
        }

        let messages: Vec<Value> = received
            .iter()
            .map(|m| {
                json!({
                    "MessageId": m.message_id,
                    "ReceiptHandle": m.receipt_handle,
                    "MD5OfBody": m.md5_of_body,
                    "Body": m.body,
                })
            })
            .collect();

        Ok(json_response(json!({ "Messages": messages })))
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
        let queue = state
            .queues
            .get_mut(queue_url)
            .ok_or_else(queue_not_found)?;

        queue
            .inflight
            .retain(|m| m.receipt_handle.as_deref() != Some(receipt_handle));

        Ok(json_response(json!({})))
    }

    fn purge_queue(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;

        let mut state = self.state.write();
        let queue = state
            .queues
            .get_mut(queue_url)
            .ok_or_else(queue_not_found)?;

        queue.messages.clear();
        queue.inflight.clear();

        Ok(json_response(json!({})))
    }

    fn change_message_visibility(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let receipt_handle = body["ReceiptHandle"]
            .as_str()
            .ok_or_else(|| missing_param("ReceiptHandle"))?;
        let visibility_timeout = body["VisibilityTimeout"]
            .as_i64()
            .ok_or_else(|| missing_param("VisibilityTimeout"))?;

        let mut state = self.state.write();
        let queue = state
            .queues
            .get_mut(queue_url)
            .ok_or_else(queue_not_found)?;

        let now = Utc::now();
        for msg in &mut queue.inflight {
            if msg.receipt_handle.as_deref() == Some(receipt_handle) {
                msg.visible_at = Some(now + chrono::Duration::seconds(visibility_timeout));
                break;
            }
        }

        Ok(json_response(json!({})))
    }
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

fn md5_hex(input: &str) -> String {
    format!("{:032x}", fxhash(input))
}

fn fxhash(input: &str) -> u128 {
    let mut hash: u128 = 0xcbf29ce484222325;
    for byte in input.bytes() {
        hash ^= byte as u128;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
