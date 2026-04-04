use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{MessageAttribute, PublishedMessage, SharedSnsState, SnsSubscription, SnsTopic};

pub struct SnsService {
    state: SharedSnsState,
    delivery: Arc<DeliveryBus>,
}

impl SnsService {
    pub fn new(state: SharedSnsState, delivery: Arc<DeliveryBus>) -> Self {
        Self { state, delivery }
    }
}

use std::sync::Arc;

#[async_trait]
impl AwsService for SnsService {
    fn service_name(&self) -> &str {
        "sns"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateTopic" => self.create_topic(&req),
            "DeleteTopic" => self.delete_topic(&req),
            "ListTopics" => self.list_topics(&req),
            "GetTopicAttributes" => self.get_topic_attributes(&req),
            "SetTopicAttributes" => self.set_topic_attributes(&req),
            "Subscribe" => self.subscribe(&req),
            "ConfirmSubscription" => self.confirm_subscription(&req),
            "Unsubscribe" => self.unsubscribe(&req),
            "Publish" => self.publish(&req),
            "ListSubscriptions" => self.list_subscriptions(&req),
            "ListSubscriptionsByTopic" => self.list_subscriptions_by_topic(&req),
            "GetSubscriptionAttributes" => self.get_subscription_attributes(&req),
            "SetSubscriptionAttributes" => self.set_subscription_attributes(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            _ => Err(AwsServiceError::action_not_implemented("sns", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateTopic",
            "DeleteTopic",
            "ListTopics",
            "GetTopicAttributes",
            "SetTopicAttributes",
            "Subscribe",
            "ConfirmSubscription",
            "Unsubscribe",
            "Publish",
            "ListSubscriptions",
            "ListSubscriptionsByTopic",
            "GetSubscriptionAttributes",
            "SetSubscriptionAttributes",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
        ]
    }
}

/// SNS uses Query protocol — params come from query_params (which includes form body).
/// But the SDK might also send JSON. We try both.
fn param(req: &AwsRequest, name: &str) -> Option<String> {
    // Try query params first (Query protocol)
    if let Some(v) = req.query_params.get(name) {
        return Some(v.clone());
    }
    // Try JSON body (JSON protocol)
    if let Ok(body) = serde_json::from_slice::<Value>(&req.body) {
        if let Some(s) = body[name].as_str() {
            return Some(s.to_string());
        }
    }
    None
}

fn required(req: &AwsRequest, name: &str) -> Result<String, AwsServiceError> {
    param(req, name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("The request must contain the parameter {name}"),
        )
    })
}

fn not_found(entity: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NotFound",
        format!("{entity} does not exist"),
    )
}

/// SNS uses XML responses for Query protocol.
fn xml_resp(inner: &str, request_id: &str) -> AwsResponse {
    let xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{inner}\n");
    let _ = request_id; // included in inner
    AwsResponse::xml(StatusCode::OK, xml)
}

impl SnsService {
    fn create_topic(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required(req, "Name")?;
        let is_fifo = name.ends_with(".fifo");

        // Parse tags from request: Tags.member.N.Key / Tags.member.N.Value
        let tags = parse_tags(req);

        let mut state = self.state.write();

        let topic_arn = format!("arn:aws:sns:{}:{}:{}", state.region, state.account_id, name);

        if !state.topics.contains_key(&topic_arn) {
            let mut attributes = HashMap::new();
            if is_fifo {
                attributes.insert("FifoTopic".to_string(), "true".to_string());
            }
            let topic = SnsTopic {
                topic_arn: topic_arn.clone(),
                name,
                attributes,
                tags,
                is_fifo,
                created_at: Utc::now(),
            };
            state.topics.insert(topic_arn.clone(), topic);
        }

        Ok(xml_resp(
            &format!(
                r#"<CreateTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreateTopicResult>
    <TopicArn>{topic_arn}</TopicArn>
  </CreateTopicResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateTopicResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn delete_topic(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let mut state = self.state.write();
        state.topics.remove(&topic_arn);
        state
            .subscriptions
            .retain(|_, sub| sub.topic_arn != topic_arn);

        Ok(xml_resp(
            &format!(
                r#"<DeleteTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeleteTopicResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_topics(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let members: String = state
            .topics
            .values()
            .map(|t| {
                format!(
                    "      <member><TopicArn>{}</TopicArn></member>",
                    t.topic_arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListTopicsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListTopicsResult>
    <Topics>
{members}
    </Topics>
  </ListTopicsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListTopicsResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_topic_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let state = self.state.read();
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        // Count confirmed and pending subscriptions for this topic
        let subs_confirmed = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.confirmed)
            .count();
        let subs_pending = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && !s.confirmed)
            .count();

        // Build default policy document that Terraform expects as valid JSON
        let default_policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"sns:Publish","Resource":"{topic_arn}"}}]}}"#,
        );

        let mut entries = vec![
            format_attr("TopicArn", &topic.topic_arn),
            format_attr("DisplayName", &topic.name),
            format_attr("Owner", &state.account_id),
            format_attr("SubscriptionsConfirmed", &subs_confirmed.to_string()),
            format_attr("SubscriptionsPending", &subs_pending.to_string()),
            format_attr("SubscriptionsDeleted", "0"),
        ];

        // Add Policy: use existing attribute if set, otherwise provide default
        if !topic.attributes.contains_key("Policy") {
            entries.push(format_attr("Policy", &default_policy));
        }

        for (k, v) in &topic.attributes {
            entries.push(format_attr(k, v));
        }

        let attrs = entries.join("\n");
        Ok(xml_resp(
            &format!(
                r#"<GetTopicAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetTopicAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetTopicAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetTopicAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn set_topic_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let attr_name = required(req, "AttributeName")?;
        let attr_value = param(req, "AttributeValue").unwrap_or_default();

        let mut state = self.state.write();
        let topic = state
            .topics
            .get_mut(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;
        topic.attributes.insert(attr_name, attr_value);

        Ok(xml_resp(
            &format!(
                r#"<SetTopicAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetTopicAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn subscribe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let protocol = required(req, "Protocol")?;
        let endpoint = param(req, "Endpoint").unwrap_or_default();

        let state_r = self.state.read();
        if !state_r.topics.contains_key(&topic_arn) {
            return Err(not_found("Topic"));
        }
        drop(state_r);

        let sub_arn = format!("{}:{}", topic_arn, uuid::Uuid::new_v4());

        let sub = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn,
            protocol,
            endpoint,
            attributes: HashMap::new(),
            confirmed: true,
        };

        self.state
            .write()
            .subscriptions
            .insert(sub_arn.clone(), sub);

        Ok(xml_resp(
            &format!(
                r#"<SubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SubscribeResult>
    <SubscriptionArn>{sub_arn}</SubscriptionArn>
  </SubscribeResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SubscribeResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn confirm_subscription(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let _token = required(req, "Token")?;

        // In a local emulator, subscriptions are auto-confirmed.
        // Find the most recent unconfirmed subscription for this topic, or just accept it.
        let state = self.state.read();
        let sub_arn = state
            .subscriptions
            .values()
            .find(|s| s.topic_arn == topic_arn)
            .map(|s| s.subscription_arn.clone())
            .unwrap_or_else(|| format!("{}:{}", topic_arn, uuid::Uuid::new_v4()));

        Ok(xml_resp(
            &format!(
                r#"<ConfirmSubscriptionResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ConfirmSubscriptionResult>
    <SubscriptionArn>{sub_arn}</SubscriptionArn>
  </ConfirmSubscriptionResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ConfirmSubscriptionResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn unsubscribe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let sub_arn = required(req, "SubscriptionArn")?;
        self.state.write().subscriptions.remove(&sub_arn);

        Ok(xml_resp(
            &format!(
                r#"<UnsubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UnsubscribeResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn publish(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let message = required(req, "Message")?;
        let subject = param(req, "Subject");
        let message_group_id = param(req, "MessageGroupId");

        // Parse MessageAttributes from query params
        let message_attributes = parse_message_attributes(req);

        let mut state = self.state.write();
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        // FIFO topic enforcement
        if topic.is_fifo && message_group_id.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "The request must contain the parameter MessageGroupId for FIFO topics",
            ));
        }

        let msg_id = uuid::Uuid::new_v4().to_string();
        state.published.push(PublishedMessage {
            message_id: msg_id.clone(),
            topic_arn: topic_arn.clone(),
            message: message.clone(),
            subject: subject.clone(),
            message_attributes: message_attributes.clone(),
            message_group_id,
            timestamp: Utc::now(),
        });

        // Collect subscribers by protocol, checking filter policies
        let sqs_subscribers: Vec<String> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.protocol == "sqs" && s.confirmed)
            .filter(|s| matches_filter_policy(s, &message_attributes))
            .map(|s| s.endpoint.clone())
            .collect();
        let http_subscribers: Vec<String> = state
            .subscriptions
            .values()
            .filter(|s| {
                s.topic_arn == topic_arn
                    && (s.protocol == "http" || s.protocol == "https")
                    && s.confirmed
            })
            .filter(|s| matches_filter_policy(s, &message_attributes))
            .map(|s| s.endpoint.clone())
            .collect();
        drop(state);

        // Build SNS notification envelope (matches real AWS format)
        let mut envelope_attrs = serde_json::Map::new();
        for (key, attr) in &message_attributes {
            let mut attr_obj = serde_json::Map::new();
            attr_obj.insert("Type".to_string(), Value::String(attr.data_type.clone()));
            if let Some(ref sv) = attr.string_value {
                attr_obj.insert("Value".to_string(), Value::String(sv.clone()));
            }
            envelope_attrs.insert(key.clone(), Value::Object(attr_obj));
        }

        let sns_envelope = serde_json::json!({
            "Type": "Notification",
            "MessageId": msg_id,
            "TopicArn": topic_arn,
            "Subject": subject.as_deref().unwrap_or(""),
            "Message": message,
            "Timestamp": Utc::now().to_rfc3339(),
            "SignatureVersion": "1",
            "Signature": "FAKE_SIGNATURE",
            "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem",
            "UnsubscribeURL": format!("http://localhost:4566/?Action=Unsubscribe&SubscriptionArn={}", topic_arn),
            "MessageAttributes": envelope_attrs,
        });
        let envelope_str = sns_envelope.to_string();

        // Deliver to SQS subscribers
        for queue_arn in &sqs_subscribers {
            self.delivery
                .send_to_sqs(queue_arn, &envelope_str, &HashMap::new());
        }

        // Deliver to HTTP/HTTPS subscribers (fire-and-forget)
        for endpoint_url in http_subscribers {
            let body = envelope_str.clone();
            let topic = topic_arn.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let result = client
                    .post(&endpoint_url)
                    .header("Content-Type", "application/json")
                    .header("x-amz-sns-message-type", "Notification")
                    .header("x-amz-sns-topic-arn", &topic)
                    .body(body)
                    .send()
                    .await;
                if let Err(e) = result {
                    tracing::warn!(endpoint = %endpoint_url, error = %e, "SNS HTTP delivery failed");
                }
            });
        }

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

    fn list_subscriptions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let members: String = state
            .subscriptions
            .values()
            .map(format_sub_member)
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListSubscriptionsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsResult>
    <Subscriptions>
{members}
    </Subscriptions>
  </ListSubscriptionsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSubscriptionsResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_subscriptions_by_topic(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let state = self.state.read();
        let members: String = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn)
            .map(format_sub_member)
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListSubscriptionsByTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsByTopicResult>
    <Subscriptions>
{members}
    </Subscriptions>
  </ListSubscriptionsByTopicResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSubscriptionsByTopicResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_subscription_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sub_arn = required(req, "SubscriptionArn")?;
        let state = self.state.read();
        let sub = state
            .subscriptions
            .get(&sub_arn)
            .ok_or_else(|| not_found("Subscription"))?;

        let mut entries = vec![
            format_attr("SubscriptionArn", &sub.subscription_arn),
            format_attr("TopicArn", &sub.topic_arn),
            format_attr("Protocol", &sub.protocol),
            format_attr("Endpoint", &sub.endpoint),
            format_attr("Owner", &state.account_id),
            format_attr("ConfirmationWasAuthenticated", "true"),
            format_attr("PendingConfirmation", "false"),
            format_attr("RawMessageDelivery", "false"),
        ];
        for (k, v) in &sub.attributes {
            entries.push(format_attr(k, v));
        }
        let attrs = entries.join("\n");

        Ok(xml_resp(
            &format!(
                r#"<GetSubscriptionAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetSubscriptionAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetSubscriptionAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSubscriptionAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn set_subscription_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sub_arn = required(req, "SubscriptionArn")?;
        let attr_name = required(req, "AttributeName")?;
        let attr_value = param(req, "AttributeValue").unwrap_or_default();

        let mut state = self.state.write();
        let sub = state
            .subscriptions
            .get_mut(&sub_arn)
            .ok_or_else(|| not_found("Subscription"))?;

        sub.attributes.insert(attr_name, attr_value);

        Ok(xml_resp(
            &format!(
                r#"<SetSubscriptionAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetSubscriptionAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let tags = parse_tags(req);

        let mut state = self.state.write();
        let topic = state
            .topics
            .get_mut(&resource_arn)
            .ok_or_else(|| not_found("Topic"))?;
        for (k, v) in tags {
            topic.tags.insert(k, v);
        }

        Ok(xml_resp(
            &format!(
                r#"<TagResourceResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <TagResourceResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</TagResourceResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let tag_keys = parse_tag_keys(req);

        let mut state = self.state.write();
        let topic = state
            .topics
            .get_mut(&resource_arn)
            .ok_or_else(|| not_found("Topic"))?;
        for key in tag_keys {
            topic.tags.remove(&key);
        }

        Ok(xml_resp(
            &format!(
                r#"<UntagResourceResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <UntagResourceResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UntagResourceResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn = required(req, "ResourceArn")?;
        let state = self.state.read();
        let topic = state
            .topics
            .get(&resource_arn)
            .ok_or_else(|| not_found("Topic"))?;

        let members: String = topic
            .tags
            .iter()
            .map(|(k, v)| format!("      <member><Key>{k}</Key><Value>{v}</Value></member>"))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListTagsForResourceResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListTagsForResourceResult>
    <Tags>
{members}
    </Tags>
  </ListTagsForResourceResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListTagsForResourceResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }
}

fn format_attr(name: &str, value: &str) -> String {
    format!("      <entry><key>{name}</key><value>{value}</value></entry>")
}

fn format_sub_member(sub: &SnsSubscription) -> String {
    format!(
        r#"      <member>
        <SubscriptionArn>{}</SubscriptionArn>
        <TopicArn>{}</TopicArn>
        <Protocol>{}</Protocol>
        <Endpoint>{}</Endpoint>
        <Owner></Owner>
      </member>"#,
        sub.subscription_arn, sub.topic_arn, sub.protocol, sub.endpoint,
    )
}

/// Parse MessageAttributes from query params.
/// Format: MessageAttributes.entry.N.Name, MessageAttributes.entry.N.Value.DataType,
///         MessageAttributes.entry.N.Value.StringValue
fn parse_message_attributes(req: &AwsRequest) -> HashMap<String, MessageAttribute> {
    let mut attrs = HashMap::new();
    for n in 1..=10 {
        let name_key = format!("MessageAttributes.entry.{n}.Name");
        let data_type_key = format!("MessageAttributes.entry.{n}.Value.DataType");
        if let (Some(name), Some(data_type)) = (
            req.query_params.get(&name_key),
            req.query_params.get(&data_type_key),
        ) {
            let string_value_key = format!("MessageAttributes.entry.{n}.Value.StringValue");
            let string_value = req.query_params.get(&string_value_key).cloned();
            attrs.insert(
                name.clone(),
                MessageAttribute {
                    data_type: data_type.clone(),
                    string_value,
                },
            );
        } else {
            break;
        }
    }
    attrs
}

/// Parse tags from query params.
/// Format: Tags.member.N.Key / Tags.member.N.Value
fn parse_tags(req: &AwsRequest) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for n in 1..=50 {
        let key_param = format!("Tags.member.{n}.Key");
        let val_param = format!("Tags.member.{n}.Value");
        if let Some(key) = req.query_params.get(&key_param) {
            let value = req
                .query_params
                .get(&val_param)
                .cloned()
                .unwrap_or_default();
            tags.insert(key.clone(), value);
        } else {
            break;
        }
    }
    tags
}

/// Parse tag keys for UntagResource.
/// Format: TagKeys.member.N
fn parse_tag_keys(req: &AwsRequest) -> Vec<String> {
    let mut keys = Vec::new();
    for n in 1..=50 {
        let key_param = format!("TagKeys.member.{n}");
        if let Some(key) = req.query_params.get(&key_param) {
            keys.push(key.clone());
        } else {
            break;
        }
    }
    keys
}

/// Check if a message's attributes match the subscription's FilterPolicy.
///
/// The FilterPolicy is a JSON object where keys are attribute names and values
/// are arrays of allowed values. A message matches if for every key in the filter,
/// the message has that attribute and its value is in the allowed list.
/// If the subscription has no FilterPolicy, all messages match.
fn matches_filter_policy(
    sub: &SnsSubscription,
    message_attributes: &HashMap<String, MessageAttribute>,
) -> bool {
    let filter_json = match sub.attributes.get("FilterPolicy") {
        Some(fp) => fp,
        None => return true, // no filter = match all
    };

    let filter: HashMap<String, Value> = match serde_json::from_str(filter_json) {
        Ok(f) => f,
        Err(_) => return true, // invalid filter = match all (lenient)
    };

    for (attr_name, allowed_values) in &filter {
        let allowed = match allowed_values.as_array() {
            Some(arr) => arr,
            None => continue, // skip non-array values
        };

        let msg_attr = match message_attributes.get(attr_name) {
            Some(a) => a,
            None => {
                // Check if any allowed value is {"exists": false} — matches when attribute is absent
                let has_exists_false = allowed.iter().any(|v| {
                    v.as_object()
                        .and_then(|o| o.get("exists"))
                        .and_then(|e| e.as_bool())
                        == Some(false)
                });
                if has_exists_false {
                    continue;
                }
                return false; // attribute missing and no exists:false matcher
            }
        };

        let attr_value = msg_attr.string_value.as_deref().unwrap_or("");
        let matched = allowed.iter().any(|v| match v {
            Value::String(s) => s == attr_value,
            Value::Number(n) => n.to_string() == attr_value,
            Value::Object(obj) => {
                // Advanced filter operators
                if let Some(prefix) = obj.get("prefix").and_then(|v| v.as_str()) {
                    attr_value.starts_with(prefix)
                } else if let Some(anything_but) = obj.get("anything-but") {
                    match anything_but {
                        Value::String(s) => attr_value != s,
                        Value::Array(arr) => !arr.iter().any(|v| v.as_str() == Some(attr_value)),
                        _ => false,
                    }
                } else if let Some(numeric_arr) = obj.get("numeric").and_then(|v| v.as_array()) {
                    // Numeric comparison: {"numeric": [">", 100]} or {"numeric": [">=", 50, "<", 200]}
                    let attr_num: f64 = match attr_value.parse() {
                        Ok(n) => n,
                        Err(_) => return false,
                    };
                    matches_numeric_filter(attr_num, numeric_arr)
                } else {
                    // {"exists": true/false} — attribute is present (since we got here), so match if true
                    obj.get("exists")
                        .and_then(|v| v.as_bool())
                        .unwrap_or_default()
                }
            }
            _ => false,
        });
        if !matched {
            return false;
        }
    }

    true
}

/// Evaluate a numeric filter like `[">", 100]` or `[">=", 50, "<", 200]` against a value.
fn matches_numeric_filter(value: f64, conditions: &[Value]) -> bool {
    let mut i = 0;
    while i + 1 < conditions.len() {
        let op = match conditions[i].as_str() {
            Some(s) => s,
            None => return false,
        };
        let threshold = match conditions[i + 1].as_f64() {
            Some(n) => n,
            None => return false,
        };
        let passes = match op {
            "=" => (value - threshold).abs() < f64::EPSILON,
            ">" => value > threshold,
            ">=" => value >= threshold,
            "<" => value < threshold,
            "<=" => value <= threshold,
            _ => return false,
        };
        if !passes {
            return false;
        }
        i += 2;
    }
    true
}
