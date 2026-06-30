//! Background runner that executes EventBridge Pipes with an SQS source.
//!
//! For each RUNNING pipe whose `Source` is an SQS queue ARN, the runner polls
//! the source queue, applies the pipe's EventBridge-pattern filter (matching
//! events are forwarded, non-matching events are acked/deleted — "filter
//! drop-as-ack", exactly as AWS Pipes does), and delivers the matching events
//! to the pipe's target (Lambda / SQS / SNS / Step Functions / EventBridge bus
//! / Kinesis) via the shared `DeliveryBus`, reusing the same cross-service
//! delivery paths every other fakecloud feature uses. A message is deleted
//! from the source only after it is either filtered out or successfully
//! delivered; a delivery failure leaves it for redelivery.
//!
//! DynamoDB-stream / Kinesis sources, enrichment, InputTemplate transforms,
//! and the remaining target types (ECS / Batch / Redshift / HTTP / …) land in
//! a later batch. Each unsupported source/target is left untouched (the pipe
//! simply does no work) rather than faking delivery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use fakecloud_core::delivery::{DeliveryBus, KmsHook};
use fakecloud_lambda::filter::FilterSet;
use fakecloud_pipes::SharedPipesState;
use fakecloud_sqs::SharedSqsState;

/// One RUNNING pipe with an SQS source, resolved for this tick.
struct SqsSourcePipe {
    source_arn: String,
    target_arn: String,
    target_params: Option<Value>,
    filter: FilterSet,
    batch_size: usize,
}

pub struct PipesRunner {
    pipes_state: SharedPipesState,
    sqs_state: SharedSqsState,
    delivery: Arc<DeliveryBus>,
    /// Decrypts at-rest SSE-KMS / SSE-SQS message bodies before forwarding,
    /// mirroring the SQS->Lambda poller: AWS consumers see plaintext, so a
    /// pipe must too rather than forward opaque envelope bytes.
    kms_hook: Option<Arc<dyn KmsHook>>,
}

impl PipesRunner {
    pub fn new(
        pipes_state: SharedPipesState,
        sqs_state: SharedSqsState,
        delivery: Arc<DeliveryBus>,
    ) -> Self {
        Self {
            pipes_state,
            sqs_state,
            delivery,
            kms_hook: None,
        }
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            self.poll().await;
        }
    }

    async fn poll(&self) {
        let pipes = self.collect_sqs_source_pipes();
        for pipe in pipes {
            self.process_pipe(&pipe).await;
        }
    }

    /// Snapshot every RUNNING pipe whose source is an SQS queue.
    fn collect_sqs_source_pipes(&self) -> Vec<SqsSourcePipe> {
        let accounts = self.pipes_state.read();
        let mut out = Vec::new();
        for state in accounts.accounts.values() {
            for pipe in state.pipes.values() {
                if pipe.get("CurrentState").and_then(Value::as_str) != Some("RUNNING") {
                    continue;
                }
                let Some(source_arn) = pipe.get("Source").and_then(Value::as_str) else {
                    continue;
                };
                if !source_arn.contains(":sqs:") {
                    continue;
                }
                let Some(target_arn) = pipe.get("Target").and_then(Value::as_str) else {
                    continue;
                };
                let source_params = pipe.get("SourceParameters");
                let filter = build_filter(source_params);
                let batch_size = source_params
                    .and_then(|p| p.get("SqsQueueParameters"))
                    .and_then(|p| p.get("BatchSize"))
                    .and_then(Value::as_i64)
                    .filter(|n| *n > 0)
                    .unwrap_or(10)
                    .min(10) as usize;
                out.push(SqsSourcePipe {
                    source_arn: source_arn.to_string(),
                    target_arn: target_arn.to_string(),
                    target_params: pipe.get("TargetParameters").cloned(),
                    filter,
                    batch_size,
                });
            }
        }
        out
    }

    async fn process_pipe(&self, pipe: &SqsSourcePipe) {
        let now = Utc::now();
        // Pull up to BatchSize visible messages and hide them for the queue's
        // visibility window so a slow delivery doesn't re-pull the same batch.
        let picked = self.pick_messages(&pipe.source_arn, pipe.batch_size, now);
        if picked.is_empty() {
            return;
        }

        // Partition by the filter: matching events are delivered, the rest are
        // acked (deleted) immediately — AWS Pipes drops filtered events.
        let mut to_deliver: Vec<(String, Value)> = Vec::new();
        let mut ack_ids: Vec<String> = Vec::new();
        for (id, event) in picked {
            if pipe.filter.is_empty() || pipe.filter.matches(&event) {
                to_deliver.push((id, event));
            } else {
                ack_ids.push(id);
            }
        }

        if !to_deliver.is_empty() {
            let delivered = self
                .deliver(&pipe.target_arn, pipe.target_params.as_ref(), &to_deliver)
                .await;
            if delivered {
                ack_ids.extend(to_deliver.into_iter().map(|(id, _)| id));
            }
        }

        if !ack_ids.is_empty() {
            self.delete_messages(&pipe.source_arn, &ack_ids);
        }
    }

    /// Pull up to `limit` currently-visible messages from the source queue and
    /// mark them invisible for the queue's `VisibilityTimeout`. Returns
    /// `(message_id, pipes-source-event)` pairs.
    fn pick_messages(
        &self,
        source_arn: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Vec<(String, Value)> {
        let mut sqs_mas = self.sqs_state.write();
        let default_acct = sqs_mas.default_account_id().to_string();
        let acct = source_arn
            .split(':')
            .nth(4)
            .unwrap_or(&default_acct)
            .to_string();
        let region = source_arn
            .split(':')
            .nth(3)
            .unwrap_or("us-east-1")
            .to_string();
        let sqs = sqs_mas.get_or_create(&acct);
        let Some(queue) = sqs.queues.values_mut().find(|q| q.arn == source_arn) else {
            return Vec::new();
        };
        let visibility_timeout: i64 = queue
            .attributes
            .get("VisibilityTimeout")
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let visible_at = now + chrono::Duration::seconds(visibility_timeout);
        // Bodies are stored encrypted at rest on an SSE-KMS / managed-SSE
        // queue; decrypt before forwarding so the target sees plaintext.
        let encrypted = queue
            .attributes
            .get("KmsMasterKeyId")
            .map(|k| !k.is_empty())
            .unwrap_or(false)
            || queue
                .attributes
                .get("SqsManagedSseEnabled")
                .map(String::as_str)
                == Some("true");

        let mut out = Vec::new();
        for msg in queue.messages.iter_mut() {
            if out.len() >= limit {
                break;
            }
            if let Some(vis) = msg.visible_at {
                if vis > now {
                    continue;
                }
            }
            msg.visible_at = Some(visible_at);
            let body = self.decrypt_body(&msg.body, source_arn, &acct, encrypted);
            out.push((
                msg.message_id.clone(),
                sqs_source_event(msg, &body, source_arn, &region),
            ));
        }
        out
    }

    /// Decrypt an at-rest SQS body when the queue is encrypted and the body
    /// is a fakecloud KMS envelope. Falls back to the raw body otherwise.
    fn decrypt_body(&self, body: &str, source_arn: &str, account: &str, encrypted: bool) -> String {
        if !encrypted || !looks_like_fakecloud_envelope(body) {
            return body.to_string();
        }
        let Some(hook) = self.kms_hook.as_ref() else {
            return body.to_string();
        };
        let mut ctx = HashMap::new();
        ctx.insert("aws:sqs:arn".to_string(), source_arn.to_string());
        match hook.decrypt(account, body, "sqs.amazonaws.com", ctx) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Err(err) => {
                tracing::warn!(%source_arn, %err, "pipes: KMS decrypt failed; forwarding opaque body");
                body.to_string()
            }
        }
    }

    fn delete_messages(&self, source_arn: &str, ids: &[String]) {
        let mut sqs_mas = self.sqs_state.write();
        let default_acct = sqs_mas.default_account_id().to_string();
        let acct = source_arn
            .split(':')
            .nth(4)
            .unwrap_or(&default_acct)
            .to_string();
        let sqs = sqs_mas.get_or_create(&acct);
        if let Some(queue) = sqs.queues.values_mut().find(|q| q.arn == source_arn) {
            queue.messages.retain(|m| !ids.contains(&m.message_id));
        }
    }

    /// Deliver the batch to the target. Returns true if delivery succeeded (and
    /// the events may be acked). Lambda receives the whole batch as a JSON
    /// array (Pipes batches to Lambda); SQS/SNS receive one message per event.
    async fn deliver(
        &self,
        target_arn: &str,
        target_params: Option<&Value>,
        batch: &[(String, Value)],
    ) -> bool {
        if target_arn.contains(":lambda:") {
            let payload = Value::Array(batch.iter().map(|(_, e)| e.clone()).collect());
            let payload = serde_json::to_string(&payload).unwrap_or_else(|_| "[]".to_string());
            match self.delivery.invoke_lambda(target_arn, &payload).await {
                Some(Ok(_)) => true,
                Some(Err(err)) => {
                    tracing::warn!(%target_arn, %err, "pipes: Lambda target invocation failed; events will be retried");
                    false
                }
                // No Lambda delivery wired (unit/no-runtime): leave events.
                None => false,
            }
        } else if target_arn.contains(":sqs:") {
            for (_, event) in batch {
                let body = serde_json::to_string(event).unwrap_or_default();
                self.delivery
                    .send_to_sqs(target_arn, &body, &HashMap::new());
            }
            true
        } else if target_arn.contains(":sns:") {
            for (_, event) in batch {
                let body = serde_json::to_string(event).unwrap_or_default();
                self.delivery.publish_to_sns(target_arn, &body, None);
            }
            true
        } else if target_arn.contains(":states:") {
            // Step Functions: start one execution per event with the event as
            // input (Pipes invokes standard state machines per record).
            for (_, event) in batch {
                let input = serde_json::to_string(event).unwrap_or_default();
                self.delivery
                    .start_stepfunctions_execution(target_arn, &input);
            }
            true
        } else if target_arn.contains(":events:") {
            // EventBridge bus: PutEvents one entry per event. Source/DetailType
            // come from the target's EventBridgeEventBusParameters (AWS
            // requires them), defaulting to Pipes' conventional values.
            let bus_name = target_arn
                .rsplit_once("event-bus/")
                .map(|(_, n)| n)
                .unwrap_or("default");
            let eb_params = target_params.and_then(|p| p.get("EventBridgeEventBusParameters"));
            let source = eb_params
                .and_then(|p| p.get("Source"))
                .and_then(Value::as_str)
                .unwrap_or("Pipes");
            let detail_type = eb_params
                .and_then(|p| p.get("DetailType"))
                .and_then(Value::as_str)
                .unwrap_or("Event");
            for (_, event) in batch {
                let detail = serde_json::to_string(event).unwrap_or_default();
                self.delivery
                    .put_event_to_eventbridge(source, detail_type, &detail, bus_name);
            }
            true
        } else if target_arn.contains(":kinesis:") {
            // Kinesis: one record per event. PartitionKey comes from the
            // target's KinesisStreamParameters (a literal here), falling back
            // to the source message id so records still spread across shards.
            let configured_pk = target_params
                .and_then(|p| p.get("KinesisStreamParameters"))
                .and_then(|p| p.get("PartitionKey"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            for (id, event) in batch {
                let data = serde_json::to_string(event).unwrap_or_default();
                let pk = configured_pk.unwrap_or(id.as_str());
                self.delivery.send_to_kinesis(target_arn, &data, pk);
            }
            true
        } else {
            // Other targets (ECS / Batch / Redshift / HTTP / SageMaker / …)
            // land in a later batch. Don't ack — leave the events so they're
            // delivered once the target type is supported, never faked.
            false
        }
    }
}

/// Build the pipe's source filter from `SourceParameters.FilterCriteria`.
fn build_filter(source_params: Option<&Value>) -> FilterSet {
    let patterns: Vec<String> = source_params
        .and_then(|p| p.get("FilterCriteria"))
        .and_then(|f| f.get("Filters"))
        .and_then(Value::as_array)
        .map(|filters| {
            filters
                .iter()
                .filter_map(|f| f.get("Pattern").and_then(Value::as_str))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();
    FilterSet::from_strings(patterns)
}

/// Build the AWS Pipes SQS-source event envelope for one message. `body` is the
/// (already decrypted) message body.
fn sqs_source_event(
    msg: &fakecloud_sqs::SqsMessage,
    body: &str,
    source_arn: &str,
    region: &str,
) -> Value {
    let attributes: serde_json::Map<String, Value> = msg
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    json!({
        "messageId": msg.message_id,
        "receiptHandle": msg.receipt_handle.clone().unwrap_or_default(),
        "body": body,
        "attributes": attributes,
        "messageAttributes": {},
        "md5OfBody": msg.md5_of_body,
        "eventSource": "aws:sqs",
        "eventSourceArn": source_arn,
        "awsRegion": region,
    })
}

/// Detect a fakecloud KMS-at-rest envelope (matches the SQS service's own
/// encryption format) so plaintext bodies are passed through untouched.
fn looks_like_fakecloud_envelope(body: &str) -> bool {
    if body.starts_with("fakecloud-kms:") {
        return true;
    }
    match base64::engine::general_purpose::STANDARD.decode(body) {
        Ok(bytes) => bytes.starts_with(&[0x01, 0x02, 0x02, 0x00]),
        Err(_) => false,
    }
}
