// Auto-extracted from service.rs as part of carryover service.rs split.
// Methods on `SnsService` grouped by resource concern.

#![allow(clippy::too_many_arguments)]

use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl SnsService {
    pub(super) fn publish(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Either TopicArn or TargetArn is required; also allow PhoneNumber for SMS
        let topic_arn = param(req, "TopicArn").or_else(|| param(req, "TargetArn"));
        let phone_number = param(req, "PhoneNumber");

        if topic_arn.is_none() && phone_number.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "The request must contain the parameter TopicArn or TargetArn or PhoneNumber",
            ));
        }

        let message = required(req, "Message")?;
        let subject = param(req, "Subject");
        let message_group_id = param(req, "MessageGroupId");
        let message_dedup_id = param(req, "MessageDeduplicationId");
        let message_structure = param(req, "MessageStructure");

        // Validate subject length
        if let Some(ref subj) = subject {
            if subj.len() > 100 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Subject must be less than 100 characters",
                ));
            }
        }

        // Validate message length (256KB)
        if message.len() > 262144 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: Message too long",
            ));
        }

        // Validate MessageStructure=json
        if message_structure.as_deref() == Some("json") {
            validate_message_structure_json(&message)?;
        }

        // Parse MessageAttributes from query params
        let message_attributes = parse_message_attributes(req);

        if let Some(ref phone) = phone_number {
            return self.publish_to_phone_number(
                req,
                phone,
                message,
                subject,
                message_attributes,
                message_group_id,
                message_dedup_id,
            );
        }

        let topic_arn = topic_arn.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "TopicArn or TargetArn is required",
            )
        })?;

        // Check if it's a platform endpoint ARN
        if topic_arn.contains(":endpoint/") {
            return self.publish_to_platform_endpoint(
                &topic_arn,
                &message,
                &message_attributes,
                &req.request_id,
            );
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Read topic fields in a tight scope so the immutable borrow ends
        // before the mutable counter re-borrow below (bug-audit 2026-05-28, 1.6).
        let (is_fifo, content_dedup, kms_key_id) = {
            let topic = state
                .topics
                .get(&topic_arn)
                .ok_or_else(|| not_found("Topic"))?;
            let content_dedup = topic
                .attributes
                .get("ContentBasedDeduplication")
                .map(|v| v == "true")
                .unwrap_or(false);
            let kms_key_id = topic
                .attributes
                .get("KmsMasterKeyId")
                .cloned()
                .filter(|s| !s.is_empty());
            (topic.is_fifo, content_dedup, kms_key_id)
        };

        // FIFO topic enforcement
        if is_fifo {
            if message_group_id.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: The request must contain the parameter MessageGroupId.",
                ));
            }
            if !content_dedup && message_dedup_id.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
        } else {
            // Non-FIFO: DeduplicationId is not allowed.
            if message_dedup_id.is_some() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: The request includes MessageDeduplicationId parameter that is not valid for this topic type",
                ));
            }
        }

        self.record_topic_kms_usage(&req.account_id, &topic_arn, kms_key_id.as_deref(), &message)?;

        // FIFO dedup (1.4). Resolve the effective dedup id: the explicit
        // MessageDeduplicationId, or the SHA-256 of the message body when
        // ContentBasedDeduplication is enabled. A duplicate within the
        // 5-minute window is suppressed (not fanned out) and replays the
        // ORIGINAL MessageId/SequenceNumber. Mirrors SQS FIFO dedup.
        let effective_dedup_id: Option<String> = if is_fifo {
            message_dedup_id.clone().or_else(|| {
                if content_dedup {
                    Some(sns_sha256_hex(&message))
                } else {
                    None
                }
            })
        } else {
            None
        };

        if let Some(ref dedup_id) = effective_dedup_id {
            let now = Utc::now();
            if let Some(topic) = state.topics.get_mut(&topic_arn) {
                topic.dedup_cache.retain(|_, e| e.expiry > now);
                if let Some(entry) = topic.dedup_cache.get(dedup_id) {
                    let sequence_xml = entry
                        .sequence_number
                        .as_deref()
                        .map(|s| format!("\n    <SequenceNumber>{s}</SequenceNumber>"))
                        .unwrap_or_default();
                    let original_id = entry.message_id.clone();
                    return Ok(xml_resp(
                        &format!(
                            r#"<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PublishResult>
    <MessageId>{original_id}</MessageId>{sequence_xml}
  </PublishResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</PublishResponse>"#,
                            req.request_id
                        ),
                        &req.request_id,
                    ));
                }
            }
        }

        // Mint a strictly-increasing FIFO SequenceNumber (1.6). None for standard topics.
        let sequence_number = if is_fifo {
            state.topics.get_mut(&topic_arn).map(|t| {
                t.fifo_sequence += 1;
                format!("{:020}", t.fifo_sequence)
            })
        } else {
            None
        };

        let msg_id = uuid::Uuid::new_v4().to_string();
        state.published.push(PublishedMessage {
            message_id: msg_id.clone(),
            topic_arn: topic_arn.clone(),
            message: message.clone(),
            subject: subject.clone(),
            message_attributes: message_attributes.clone(),
            message_group_id: message_group_id.clone(),
            message_dedup_id: message_dedup_id.clone(),
            timestamp: Utc::now(),
        });

        // Record this publish in the FIFO dedup cache so a duplicate within
        // the 5-minute window replays it (1.4).
        if let Some(ref dedup_id) = effective_dedup_id {
            if let Some(topic) = state.topics.get_mut(&topic_arn) {
                topic.dedup_cache.insert(
                    dedup_id.clone(),
                    crate::state::SnsDedupEntry {
                        message_id: msg_id.clone(),
                        sequence_number: sequence_number.clone(),
                        expiry: Utc::now() + chrono::Duration::minutes(5),
                    },
                );
            }
        }

        // Resolve the actual message per protocol for MessageStructure=json
        let parsed_structure: Option<Value> = if message_structure.as_deref() == Some("json") {
            Some(serde_json::from_str(&message).map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: Message Structure - No JSON message body is parseable",
                )
            })?)
        } else {
            None
        };

        let subscribers =
            collect_topic_subscribers(state, &topic_arn, &message_attributes, &message);
        let endpoint = state.endpoint.clone();
        drop(accounts);

        // Determine the fallback ("default" key) body; each fan-out helper
        // resolves its own protocol-specific body from `parsed_structure`.
        let default_message = if let Some(ref structure) = parsed_structure {
            structure
                .get("default")
                .and_then(|v| v.as_str())
                .unwrap_or(&message)
                .to_string()
        } else {
            message.clone()
        };

        let envelope_attrs = build_envelope_attrs(&message_attributes);

        let ctx = TopicFanoutContext {
            msg_id: &msg_id,
            topic_arn: &topic_arn,
            subject: subject.as_deref(),
            endpoint: &endpoint,
            default_message: &default_message,
            structure: parsed_structure.as_ref(),
            envelope_attrs: &envelope_attrs,
            message_attributes: &message_attributes,
            message_group_id: message_group_id.as_deref(),
            message_dedup_id: message_dedup_id.as_deref(),
        };

        deliver_to_sqs_subscribers(&self.delivery, &subscribers.sqs, &ctx);
        deliver_to_http_subscribers(&self.delivery, &subscribers.http, &ctx);
        deliver_to_lambda_subscribers(&self.state, &self.delivery, &subscribers.lambda, &ctx);
        deliver_to_email_subscribers(&self.state, &subscribers.email, &ctx);
        deliver_to_sms_subscribers(&self.state, &subscribers.sms, &ctx);

        let sequence_xml = sequence_number
            .as_deref()
            .map(|s| format!("\n    <SequenceNumber>{s}</SequenceNumber>"))
            .unwrap_or_default();
        Ok(xml_resp(
            &format!(
                r#"<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PublishResult>
    <MessageId>{msg_id}</MessageId>{sequence_xml}
  </PublishResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</PublishResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn publish_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;

        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;
        let is_fifo = topic.is_fifo;
        let content_dedup = topic
            .attributes
            .get("ContentBasedDeduplication")
            .map(|v| v == "true")
            .unwrap_or(false);
        let endpoint = state.endpoint.clone();
        drop(_accts);

        // Parse batch entries: PublishBatchRequestEntries.member.N.*
        let mut entries = Vec::new();
        for n in 1..=100 {
            let id_key = format!("PublishBatchRequestEntries.member.{n}.Id");
            if let Some(id) = req.query_params.get(&id_key) {
                let msg_key = format!("PublishBatchRequestEntries.member.{n}.Message");
                let message = req.query_params.get(&msg_key).cloned().unwrap_or_default();
                let subject_key = format!("PublishBatchRequestEntries.member.{n}.Subject");
                let subject = req.query_params.get(&subject_key).cloned();
                let group_key = format!("PublishBatchRequestEntries.member.{n}.MessageGroupId");
                let group_id = req.query_params.get(&group_key).cloned();
                let dedup_key =
                    format!("PublishBatchRequestEntries.member.{n}.MessageDeduplicationId");
                let dedup_id = req.query_params.get(&dedup_key).cloned();
                let structure_key =
                    format!("PublishBatchRequestEntries.member.{n}.MessageStructure");
                let message_structure = req.query_params.get(&structure_key).cloned();
                entries.push((
                    id.clone(),
                    message,
                    subject,
                    group_id,
                    dedup_id,
                    message_structure,
                ));
            } else {
                break;
            }
        }

        // Validate: at least one entry. AWS returns EmptyBatchRequest for
        // a batch with no entries rather than silently succeeding.
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EmptyBatchRequest",
                "The batch request doesn't contain any entries.",
            ));
        }

        // Validate: max 10 entries
        if entries.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TooManyEntriesInBatchRequest",
                "The batch request contains more entries than permissible.",
            ));
        }

        // Validate: unique IDs
        let ids: Vec<&str> = entries.iter().map(|e| e.0.as_str()).collect();
        let unique_ids: std::collections::HashSet<&str> = ids.iter().copied().collect();
        if unique_ids.len() != ids.len() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BatchEntryIdsNotDistinct",
                "Two or more batch entries in the request have the same Id.",
            ));
        }

        // FIFO: all entries must have MessageGroupId — this is a top-level error
        if is_fifo && entries.iter().any(|e| e.3.is_none()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: The MessageGroupId parameter is required for FIFO topics",
            ));
        }

        // FIFO without ContentBasedDeduplication: every entry must carry a
        // MessageDeduplicationId (matches the single Publish validation).
        if is_fifo && !content_dedup && entries.iter().any(|e| e.4.is_none()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
            ));
        }

        let mut successful = Vec::new();
        let failed: Vec<String> = Vec::new();

        for (idx, (id, message, subject, group_id, dedup_id, structure)) in
            entries.iter().enumerate()
        {
            // Parse per-entry message attributes
            let batch_attrs = parse_batch_message_attributes(req, idx + 1);

            // Validate MessageStructure=json
            if structure.as_deref() == Some("json") {
                validate_message_structure_json(message)?;
            }

            // FIFO dedup + sequence minting, mirroring single Publish. A
            // duplicate within the 5-minute window replays the original
            // MessageId/SequenceNumber and is NOT fanned out again — the batch
            // path previously skipped dedup entirely.
            let effective_dedup_id: Option<String> = if is_fifo {
                dedup_id
                    .clone()
                    .or_else(|| content_dedup.then(|| sns_sha256_hex(message)))
            } else {
                None
            };
            let (msg_id, sequence_number, deduped) = {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&req.account_id);
                let now = Utc::now();
                let replayed = effective_dedup_id.as_ref().and_then(|dedup_id| {
                    let topic = state.topics.get_mut(&topic_arn)?;
                    topic.dedup_cache.retain(|_, e| e.expiry > now);
                    let entry = topic.dedup_cache.get(dedup_id)?;
                    Some((entry.message_id.clone(), entry.sequence_number.clone()))
                });
                if let Some((orig_id, orig_seq)) = replayed {
                    (orig_id, orig_seq, true)
                } else {
                    let sequence_number = if is_fifo {
                        state.topics.get_mut(&topic_arn).map(|t| {
                            t.fifo_sequence += 1;
                            format!("{:020}", t.fifo_sequence)
                        })
                    } else {
                        None
                    };
                    let msg_id = uuid::Uuid::new_v4().to_string();
                    state.published.push(PublishedMessage {
                        message_id: msg_id.clone(),
                        topic_arn: topic_arn.clone(),
                        message: message.clone(),
                        subject: subject.clone(),
                        message_attributes: batch_attrs.clone(),
                        message_group_id: group_id.clone(),
                        message_dedup_id: dedup_id.clone(),
                        timestamp: now,
                    });
                    if let Some(ref dedup_id) = effective_dedup_id {
                        if let Some(topic) = state.topics.get_mut(&topic_arn) {
                            topic.dedup_cache.insert(
                                dedup_id.clone(),
                                crate::state::SnsDedupEntry {
                                    message_id: msg_id.clone(),
                                    sequence_number: sequence_number.clone(),
                                    expiry: now + chrono::Duration::minutes(5),
                                },
                            );
                        }
                    }
                    (msg_id, sequence_number, false)
                }
            };

            // Resolve MessageStructure=json once for this entry.
            let parsed_structure: Option<Value> = if structure.as_deref() == Some("json") {
                Some(serde_json::from_str(message).map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        "Invalid parameter: Message Structure - No JSON message body is parseable",
                    )
                })?)
            } else {
                None
            };
            let default_message = if let Some(ref s) = parsed_structure {
                s.get("default")
                    .and_then(|v| v.as_str())
                    .unwrap_or(message)
                    .to_string()
            } else {
                message.clone()
            };

            // Fan out to ALL subscriber protocols, applying each
            // subscription's FilterPolicy — mirroring single Publish. The
            // batch path previously hand-filtered to SQS only, so HTTP /
            // Lambda / email / SMS subscribers got nothing and FilterPolicy
            // was never evaluated. A deduplicated entry is not re-delivered.
            if !deduped {
                let subscribers = {
                    let accts = self.state.read();
                    let empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
                    let state = accts.get(&req.account_id).unwrap_or(&empty);
                    collect_topic_subscribers(state, &topic_arn, &batch_attrs, message)
                };
                let envelope_attrs = build_envelope_attrs(&batch_attrs);
                let ctx = TopicFanoutContext {
                    msg_id: &msg_id,
                    topic_arn: &topic_arn,
                    subject: subject.as_deref(),
                    endpoint: &endpoint,
                    default_message: &default_message,
                    structure: parsed_structure.as_ref(),
                    envelope_attrs: &envelope_attrs,
                    message_attributes: &batch_attrs,
                    message_group_id: if is_fifo { group_id.as_deref() } else { None },
                    message_dedup_id: if is_fifo { dedup_id.as_deref() } else { None },
                };
                deliver_to_sqs_subscribers(&self.delivery, &subscribers.sqs, &ctx);
                deliver_to_http_subscribers(&self.delivery, &subscribers.http, &ctx);
                deliver_to_lambda_subscribers(
                    &self.state,
                    &self.delivery,
                    &subscribers.lambda,
                    &ctx,
                );
                deliver_to_email_subscribers(&self.state, &subscribers.email, &ctx);
                deliver_to_sms_subscribers(&self.state, &subscribers.sms, &ctx);
            }

            let sequence_xml = sequence_number
                .as_deref()
                .map(|seq| format!("\n      <SequenceNumber>{seq}</SequenceNumber>"))
                .unwrap_or_default();
            successful.push(format!(
                r#"    <member>
      <Id>{id}</Id>
      <MessageId>{msg_id}</MessageId>{sequence_xml}
    </member>"#
            ));
        }

        let successful_xml = successful.join("\n");
        let failed_xml = failed.join("\n");

        Ok(xml_resp(
            &format!(
                r#"<PublishBatchResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PublishBatchResult>
    <Successful>
{successful_xml}
    </Successful>
    <Failed>
{failed_xml}
    </Failed>
  </PublishBatchResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</PublishBatchResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    /// Publish directly to an SMS destination (`Publish` called with
    /// `PhoneNumber` instead of `TopicArn` / `TargetArn`). This path
    /// does its own length and E.164 validation since AWS reports
    /// distinct error messages for SMS.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn publish_to_phone_number(
        &self,
        req: &AwsRequest,
        phone: &str,
        message: String,
        subject: Option<String>,
        message_attributes: BTreeMap<String, MessageAttribute>,
        message_group_id: Option<String>,
        message_dedup_id: Option<String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let is_valid_e164 = phone.starts_with('+')
            && phone.len() >= 2
            && phone[1..].chars().all(|c| c.is_ascii_digit());
        if !is_valid_e164 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: PhoneNumber Reason: {phone} does not meet the E164 format"
                ),
            ));
        }

        if message.len() > 1600 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: Message Reason: Message must be less than 1600 characters long",
            ));
        }

        let msg_id = uuid::Uuid::new_v4().to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .sms_messages
            .push((phone.to_string(), message.clone()));
        state.published.push(PublishedMessage {
            message_id: msg_id.clone(),
            topic_arn: String::new(),
            message,
            subject,
            message_attributes,
            message_group_id,
            message_dedup_id,
            timestamp: Utc::now(),
        });

        Ok(xml_resp(
            &format!(
                r#"<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PublishResult>
    <MessageId>{msg_id}</MessageId>
  </PublishResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</PublishResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn publish_to_platform_endpoint(
        &self,
        endpoint_arn: &str,
        message: &str,
        message_attributes: &BTreeMap<String, MessageAttribute>,
        request_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let acct = endpoint_arn.split(':').nth(4).unwrap_or("");
        let _accts = self.state.read();
        let state = _accts.get(acct).unwrap_or_else(|| _accts.default_ref());

        // Find the platform endpoint
        let mut found_endpoint: Option<&PlatformEndpoint> = None;
        for app in state.platform_applications.values() {
            if let Some(ep) = app.endpoints.get(endpoint_arn) {
                found_endpoint = Some(ep);
                break;
            }
        }

        let ep = found_endpoint.ok_or_else(|| {
            AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFound", "Endpoint does not exist")
        })?;

        if !ep.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EndpointDisabled",
                "Endpoint is disabled",
            ));
        }
        drop(_accts);

        let msg_id = uuid::Uuid::new_v4().to_string();
        let acct = endpoint_arn.split(':').nth(4).unwrap_or("");
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(acct);
        // Store message on the endpoint
        for app in state.platform_applications.values_mut() {
            if let Some(ep) = app.endpoints.get_mut(endpoint_arn) {
                ep.messages.push(PublishedMessage {
                    message_id: msg_id.clone(),
                    topic_arn: endpoint_arn.to_string(),
                    message: message.to_string(),
                    subject: None,
                    message_attributes: message_attributes.clone(),
                    message_group_id: None,
                    message_dedup_id: None,
                    timestamp: Utc::now(),
                });
                break;
            }
        }

        Ok(xml_resp(
            &format!(
                r#"<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PublishResult>
    <MessageId>{msg_id}</MessageId>
  </PublishResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</PublishResponse>"#,
            ),
            request_id,
        ))
    }
}

/// Fan out a single resolved SNS message to every protocol the topic has
/// subscribers on. Used by both the direct `Publish` op and the
/// cross-service `SnsDelivery` trait so filter policies, FIFO group/dedup
/// IDs, HTTP retries, and Lambda invocations stay identical between the
/// two entry points.
pub(crate) fn fan_out_to_subscribers(
    state: &SharedSnsState,
    delivery: &Arc<DeliveryBus>,
    subscribers: &TopicSubscribers,
    ctx: &TopicFanoutContext<'_>,
) {
    deliver_to_sqs_subscribers(delivery, &subscribers.sqs, ctx);
    deliver_to_http_subscribers(delivery, &subscribers.http, ctx);
    deliver_to_lambda_subscribers(state, delivery, &subscribers.lambda, ctx);
    deliver_to_email_subscribers(state, &subscribers.email, ctx);
    deliver_to_sms_subscribers(state, &subscribers.sms, ctx);
}

pub(crate) fn deliver_to_sqs_subscribers(
    delivery: &Arc<DeliveryBus>,
    subs: &[(String, bool, String, Option<String>)],
    ctx: &TopicFanoutContext<'_>,
) {
    let sqs_message = ctx.body_for_protocol("sqs");
    for (queue_arn, raw, subscription_arn, redrive_policy) in subs {
        let (body, attrs) = if *raw {
            let mut sqs_msg_attrs = HashMap::new();
            for (k, v) in ctx.message_attributes {
                let mut attr = fakecloud_core::delivery::SqsMessageAttribute {
                    data_type: v.data_type.clone(),
                    string_value: v.string_value.clone(),
                    binary_value: None,
                };
                if let Some(ref bv) = v.binary_value {
                    attr.binary_value = Some(base64::engine::general_purpose::STANDARD.encode(bv));
                }
                sqs_msg_attrs.insert(k.clone(), attr);
            }
            (sqs_message.clone(), sqs_msg_attrs)
        } else {
            let envelope_str = build_sns_envelope(
                ctx.msg_id,
                ctx.topic_arn,
                subscription_arn,
                &ctx.subject.map(|s| s.to_string()),
                &sqs_message,
                ctx.envelope_attrs,
                ctx.endpoint,
            );
            (envelope_str, HashMap::new())
        };
        // Even non-raw delivery forwards FIFO ordering metadata so the
        // downstream queue can preserve group ordering / dedup — SNS does this
        // on real AWS. A fallible send lets us route to the subscription's DLQ
        // when the target queue is gone, instead of silently dropping (the DLQ
        // was previously honored for HTTP subscribers only).
        let result = delivery.try_send_to_sqs_with_attrs(
            queue_arn,
            &body,
            &attrs,
            ctx.message_group_id,
            ctx.message_dedup_id,
        );
        if let Err(err) = result {
            if let Some(dlq_arn) = parse_redrive_dlq(redrive_policy.as_deref()) {
                let dlq_body = build_dlq_envelope(&body, &err.to_string(), subscription_arn);
                delivery.send_to_sqs(&dlq_arn, &dlq_body, &HashMap::new());
            } else {
                tracing::warn!(
                    queue = %queue_arn,
                    error = %err,
                    "SNS SQS delivery failed and no RedrivePolicy configured; message dropped"
                );
            }
        }
    }
}

pub(crate) fn deliver_to_http_subscribers(
    delivery: &Arc<DeliveryBus>,
    subs: &[crate::service::HttpSubscriber],
    ctx: &TopicFanoutContext<'_>,
) {
    for sub in subs {
        let protocol_body = ctx.body_for_protocol(&sub.protocol);
        let body = build_sns_envelope(
            ctx.msg_id,
            ctx.topic_arn,
            &sub.subscription_arn,
            &ctx.subject.map(|s| s.to_string()),
            &protocol_body,
            ctx.envelope_attrs,
            ctx.endpoint,
        );
        let topic = ctx.topic_arn.to_string();
        let sub = sub.clone();
        let delivery = delivery.clone();
        tokio::spawn(async move {
            let policy = parse_http_delivery_policy(sub.delivery_policy.as_deref());
            let client = reqwest::Client::new();
            let mut last_err: Option<String> = None;
            let attempts = policy.num_retries.saturating_add(1);
            for attempt in 0..attempts {
                if attempt > 0 {
                    let delay_ms = retry_delay_ms(&policy, attempt);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                let resp = client
                    .post(&sub.endpoint)
                    .header("Content-Type", "application/json")
                    .header("x-amz-sns-message-type", "Notification")
                    .header("x-amz-sns-topic-arn", &topic)
                    .header("x-amz-sns-subscription-arn", &sub.subscription_arn)
                    .body(body.clone())
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => return,
                    Ok(r) => last_err = Some(format!("HTTP {}", r.status())),
                    Err(e) => last_err = Some(e.to_string()),
                }
            }
            let err = last_err.unwrap_or_else(|| "unknown".to_string());
            tracing::warn!(
                endpoint = %sub.endpoint,
                error = %err,
                "SNS HTTP delivery exhausted retries"
            );
            if let Some(dlq_arn) = parse_redrive_dlq(sub.redrive_policy.as_deref()) {
                let dlq_body = build_dlq_envelope(&body, &err, &sub.subscription_arn);
                delivery.send_to_sqs(&dlq_arn, &dlq_body, &std::collections::HashMap::new());
            }
        });
    }
}

pub(crate) fn deliver_to_lambda_subscribers(
    state: &SharedSnsState,
    delivery: &Arc<DeliveryBus>,
    subs: &[(String, String, Option<String>)],
    ctx: &TopicFanoutContext<'_>,
) {
    if subs.is_empty() {
        return;
    }
    let now = Utc::now();
    let subject_owned = ctx.subject.map(|s| s.to_string());
    let lambda_message = ctx.body_for_protocol("lambda");

    // (function_arn, payload, subscription_arn, redrive_policy)
    let lambda_payloads: Vec<(String, String, String, Option<String>)> = subs
        .iter()
        .map(|(function_arn, subscription_arn, redrive_policy)| {
            let payload = build_sns_lambda_event(&SnsLambdaEventInput {
                message_id: ctx.msg_id,
                topic_arn: ctx.topic_arn,
                subscription_arn,
                message: &lambda_message,
                subject: ctx.subject,
                message_attributes: ctx.envelope_attrs,
                timestamp: &now,
                endpoint: ctx.endpoint,
            });
            (
                function_arn.clone(),
                payload,
                subscription_arn.clone(),
                redrive_policy.clone(),
            )
        })
        .collect();

    {
        let acct = ctx.topic_arn.split(':').nth(4).unwrap_or("");
        let mut accounts = state.write();
        let state = accounts.get_or_create(acct);
        for (function_arn, ..) in &lambda_payloads {
            state
                .lambda_invocations
                .push(crate::state::LambdaInvocation {
                    function_arn: function_arn.clone(),
                    message: lambda_message.clone(),
                    subject: subject_owned.clone(),
                    timestamp: now,
                });
        }
    }

    let delivery = delivery.clone();
    tokio::spawn(async move {
        for (function_arn, payload, subscription_arn, redrive_policy) in lambda_payloads {
            tracing::info!(function_arn = %function_arn, "SNS invoking Lambda function");
            match delivery.invoke_lambda(&function_arn, &payload).await {
                Some(Ok(_)) => {
                    tracing::info!(
                        function_arn = %function_arn,
                        "SNS->Lambda invocation succeeded"
                    );
                }
                Some(Err(e)) => {
                    tracing::error!(
                        function_arn = %function_arn,
                        error = %e,
                        "SNS->Lambda invocation failed"
                    );
                    // Route the failed notification to the subscription's DLQ
                    // (previously only HTTP subscribers honored RedrivePolicy).
                    if let Some(dlq_arn) = parse_redrive_dlq(redrive_policy.as_deref()) {
                        let dlq_body =
                            build_dlq_envelope(&payload, &e.to_string(), &subscription_arn);
                        delivery.send_to_sqs(&dlq_arn, &dlq_body, &HashMap::new());
                    }
                }
                None => {
                    tracing::debug!(
                        function_arn = %function_arn,
                        "SNS->Lambda: no container runtime, skipping real execution"
                    );
                }
            }
        }
    });
}

pub(crate) fn deliver_to_email_subscribers(
    state: &SharedSnsState,
    subs: &[(String, String)],
    ctx: &TopicFanoutContext<'_>,
) {
    if subs.is_empty() {
        return;
    }
    let now = Utc::now();
    let subject_owned = ctx.subject.map(|s| s.to_string());
    let topic_arn = ctx.topic_arn.to_string();
    // Resolve the body per protocol so `email` and `email-json` subscribers
    // can each receive their own MessageStructure=json variant.
    let email_body = ctx.body_for_protocol("email");
    let email_json_body = ctx.body_for_protocol("email-json");
    let body_for = |protocol: &str| -> String {
        if protocol == "email-json" {
            email_json_body.clone()
        } else {
            email_body.clone()
        }
    };
    let acct = ctx.topic_arn.split(':').nth(4).unwrap_or("");
    {
        let mut accounts = state.write();
        let state = accounts.get_or_create(acct);
        for (email_address, protocol) in subs {
            tracing::info!(
                email = %email_address,
                topic_arn = %topic_arn,
                "SNS delivering to email"
            );
            state.sent_emails.push(crate::state::SentEmail {
                email_address: email_address.clone(),
                message: body_for(protocol),
                subject: subject_owned.clone(),
                topic_arn: topic_arn.clone(),
                timestamp: now,
            });
        }
    }

    // Opt-in SMTP relay: when FAKECLOUD_SES_SMTP_RELAY is set, fire the
    // notification via plain SMTP so dev setups receive real mail in
    // their MTA (Mailpit / MailHog). Reuses the SES helper so the
    // behavior matches SES SendEmail.
    if let Ok(relay_url) = std::env::var("FAKECLOUD_SES_SMTP_RELAY") {
        let subject = subject_owned
            .clone()
            .unwrap_or_else(|| format!("AWS Notification - {topic_arn}"));
        let from = format!("no-reply@sns.{}.amazonaws.com", default_region(&topic_arn));
        for (email_address, protocol) in subs {
            let to = [email_address.clone()];
            let relay_body = body_for(protocol);
            let mail = fakecloud_ses::smtp_relay::OutboundMail {
                from: &from,
                to: &to,
                cc: &[],
                bcc: &[],
                subject: Some(&subject),
                text_body: Some(&relay_body),
                html_body: None,
            };
            if let Err(err) = fakecloud_ses::smtp_relay::relay(&relay_url, &mail) {
                tracing::warn!(
                    relay = %relay_url,
                    email = %email_address,
                    error = %err,
                    "SNS email SMTP relay failed"
                );
            }
        }
    }
}

fn default_region(topic_arn: &str) -> String {
    topic_arn
        .split(':')
        .nth(3)
        .filter(|s| !s.is_empty())
        .unwrap_or("us-east-1")
        .to_string()
}

pub(crate) fn deliver_to_sms_subscribers(
    state: &SharedSnsState,
    subs: &[String],
    ctx: &TopicFanoutContext<'_>,
) {
    if subs.is_empty() {
        return;
    }
    let sms_message = ctx.body_for_protocol("sms");
    let acct = ctx.topic_arn.split(':').nth(4).unwrap_or("");
    let mut accounts = state.write();
    let state = accounts.get_or_create(acct);
    for phone_number in subs {
        tracing::info!(
            phone_number = %phone_number,
            topic_arn = %ctx.topic_arn,
            "SNS delivering to SMS (stub)"
        );
        state
            .sms_messages
            .push((phone_number.clone(), sms_message.clone()));
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpDeliveryPolicy {
    pub(crate) num_retries: u32,
    pub(crate) min_delay_target: u32,
    pub(crate) max_delay_target: u32,
}

impl Default for HttpDeliveryPolicy {
    fn default() -> Self {
        Self {
            num_retries: 3,
            min_delay_target: 20,
            max_delay_target: 20,
        }
    }
}

/// Parse subscription DeliveryPolicy JSON into the retry knobs SNS
/// honours: numRetries, minDelayTarget, maxDelayTarget. Falls back to
/// SNS's documented defaults (3 retries, 20s) on missing/malformed.
pub(crate) fn parse_http_delivery_policy(raw: Option<&str>) -> HttpDeliveryPolicy {
    let mut out = HttpDeliveryPolicy::default();
    let Some(s) = raw else { return out };
    let Ok(v) = serde_json::from_str::<Value>(s) else {
        return out;
    };
    let healthy = v.get("healthyRetryPolicy").or(Some(&v));
    if let Some(p) = healthy {
        if let Some(n) = p.get("numRetries").and_then(|x| x.as_u64()) {
            out.num_retries = n.min(100) as u32;
        }
        if let Some(n) = p.get("minDelayTarget").and_then(|x| x.as_u64()) {
            out.min_delay_target = n as u32;
        }
        if let Some(n) = p.get("maxDelayTarget").and_then(|x| x.as_u64()) {
            out.max_delay_target = n as u32;
        }
    }
    out
}

/// Linear backoff between min and max delay target across attempts. SNS
/// uses an exponential or linear policy depending on `backoffFunction`;
/// fakecloud picks linear (the default) since that's what most callers
/// configure and the variance doesn't change retry semantics.
pub(crate) fn retry_delay_ms(policy: &HttpDeliveryPolicy, attempt: u32) -> u64 {
    let span = policy
        .max_delay_target
        .saturating_sub(policy.min_delay_target);
    let extra = if policy.num_retries == 0 {
        0
    } else {
        span as u64 * attempt as u64 / policy.num_retries as u64
    };
    // Multiply seconds by 50ms for fakecloud — real AWS sleeps full
    // seconds, but tests need to finish quickly; logical retry shape
    // (attempt count, DLQ routing) matters more than wall-clock delay.
    (policy.min_delay_target as u64 + extra) * 50
}

/// Pull the SQS DLQ ARN out of a RedrivePolicy JSON like
/// `{"deadLetterTargetArn": "arn:aws:sqs:..."}`. AWS only supports SQS
/// queues here; anything else returns None.
pub(crate) fn parse_redrive_dlq(raw: Option<&str>) -> Option<String> {
    let s = raw?;
    let v: Value = serde_json::from_str(s).ok()?;
    v.get("deadLetterTargetArn")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
}

/// Wrap the original SNS notification body in the metadata SNS attaches
/// when forwarding to a redrive DLQ.
pub(crate) fn build_dlq_envelope(
    original_body: &str,
    error: &str,
    subscription_arn: &str,
) -> String {
    let original: Value = serde_json::from_str(original_body)
        .unwrap_or_else(|_| Value::String(original_body.to_string()));
    serde_json::to_string(&serde_json::json!({
        "Message": original,
        "DeliveryError": error,
        "SubscriptionArn": subscription_arn,
    }))
    .unwrap_or_else(|_| original_body.to_string())
}

#[cfg(test)]
mod retry_dlq_tests {
    use super::*;

    #[test]
    fn parse_http_delivery_policy_falls_back_on_missing() {
        let p = parse_http_delivery_policy(None);
        assert_eq!(p.num_retries, 3);
        assert_eq!(p.min_delay_target, 20);
    }

    #[test]
    fn parse_http_delivery_policy_reads_healthy_retry_block() {
        let p = parse_http_delivery_policy(Some(
            r#"{"healthyRetryPolicy":{"numRetries":7,"minDelayTarget":1,"maxDelayTarget":5}}"#,
        ));
        assert_eq!(p.num_retries, 7);
        assert_eq!(p.min_delay_target, 1);
        assert_eq!(p.max_delay_target, 5);
    }

    #[test]
    fn parse_http_delivery_policy_reads_top_level_block() {
        let p = parse_http_delivery_policy(Some(
            r#"{"numRetries":2,"minDelayTarget":1,"maxDelayTarget":4}"#,
        ));
        assert_eq!(p.num_retries, 2);
        assert_eq!(p.min_delay_target, 1);
    }

    #[test]
    fn retry_delay_ramps_between_min_and_max() {
        let p = HttpDeliveryPolicy {
            num_retries: 4,
            min_delay_target: 1,
            max_delay_target: 5,
        };
        assert!(retry_delay_ms(&p, 1) >= retry_delay_ms(&p, 0));
        assert!(retry_delay_ms(&p, 4) >= retry_delay_ms(&p, 1));
    }

    #[test]
    fn parse_redrive_dlq_extracts_arn() {
        assert_eq!(
            parse_redrive_dlq(Some(
                r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq"}"#
            )),
            Some("arn:aws:sqs:us-east-1:1:dlq".to_string())
        );
    }

    #[test]
    fn parse_redrive_dlq_returns_none_for_garbage() {
        assert!(parse_redrive_dlq(Some("not json")).is_none());
        assert!(parse_redrive_dlq(None).is_none());
    }

    #[test]
    fn build_dlq_envelope_wraps_original_with_metadata() {
        let body = build_dlq_envelope(r#"{"Message":"hi"}"#, "HTTP 500", "arn:sub");
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["DeliveryError"], "HTTP 500");
        assert_eq!(v["SubscriptionArn"], "arn:sub");
        assert_eq!(v["Message"]["Message"], "hi");
    }
}
