use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    MessageAttribute, PlatformApplication, PlatformEndpoint, PublishedMessage, SharedSnsState,
    SnsSubscription, SnsTopic,
};

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

const DEFAULT_PAGE_SIZE: usize = 100;

const DEFAULT_EFFECTIVE_DELIVERY_POLICY: &str = r#"{"defaultHealthyRetryPolicy":{"numNoDelayRetries":0,"numMinDelayRetries":0,"minDelayTarget":20,"maxDelayTarget":20,"numMaxDelayRetries":0,"numRetries":3,"backoffFunction":"linear"},"sicklyRetryPolicy":null,"throttlePolicy":null,"guaranteed":false}"#;

fn default_policy(topic_arn: &str, account_id: &str) -> String {
    serde_json::json!({
        "Version": "2008-10-17",
        "Id": "__default_policy_ID",
        "Statement": [{
            "Effect": "Allow",
            "Sid": "__default_statement_ID",
            "Principal": {"AWS": "*"},
            "Action": [
                "SNS:GetTopicAttributes",
                "SNS:SetTopicAttributes",
                "SNS:AddPermission",
                "SNS:RemovePermission",
                "SNS:DeleteTopic",
                "SNS:Subscribe",
                "SNS:ListSubscriptionsByTopic",
                "SNS:Publish",
            ],
            "Resource": topic_arn,
            "Condition": {"StringEquals": {"AWS:SourceOwner": account_id}},
        }]
    })
    .to_string()
}

const VALID_SNS_ACTIONS: &[&str] = &[
    "GetTopicAttributes",
    "SetTopicAttributes",
    "AddPermission",
    "RemovePermission",
    "DeleteTopic",
    "Subscribe",
    "ListSubscriptionsByTopic",
    "Publish",
    "Receive",
];

const VALID_SUBSCRIPTION_ATTRS: &[&str] = &[
    "RawMessageDelivery",
    "DeliveryPolicy",
    "FilterPolicy",
    "FilterPolicyScope",
    "RedrivePolicy",
    "SubscriptionRoleArn",
];

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
            "PublishBatch" => self.publish_batch(&req),
            "ListSubscriptions" => self.list_subscriptions(&req),
            "ListSubscriptionsByTopic" => self.list_subscriptions_by_topic(&req),
            "GetSubscriptionAttributes" => self.get_subscription_attributes(&req),
            "SetSubscriptionAttributes" => self.set_subscription_attributes(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "AddPermission" => self.add_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            // Platform application actions
            "CreatePlatformApplication" => self.create_platform_application(&req),
            "DeletePlatformApplication" => self.delete_platform_application(&req),
            "GetPlatformApplicationAttributes" => self.get_platform_application_attributes(&req),
            "SetPlatformApplicationAttributes" => self.set_platform_application_attributes(&req),
            "ListPlatformApplications" => self.list_platform_applications(&req),
            "CreatePlatformEndpoint" => self.create_platform_endpoint(&req),
            "DeleteEndpoint" => self.delete_endpoint(&req),
            "GetEndpointAttributes" => self.get_endpoint_attributes(&req),
            "SetEndpointAttributes" => self.set_endpoint_attributes(&req),
            "ListEndpointsByPlatformApplication" => {
                self.list_endpoints_by_platform_application(&req)
            }
            // SMS actions
            "SetSMSAttributes" => self.set_sms_attributes(&req),
            "GetSMSAttributes" => self.get_sms_attributes(&req),
            "CheckIfPhoneNumberIsOptedOut" => self.check_if_phone_number_is_opted_out(&req),
            "ListPhoneNumbersOptedOut" => self.list_phone_numbers_opted_out(&req),
            "OptInPhoneNumber" => self.opt_in_phone_number(&req),
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
            "PublishBatch",
            "ListSubscriptions",
            "ListSubscriptionsByTopic",
            "GetSubscriptionAttributes",
            "SetSubscriptionAttributes",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "AddPermission",
            "RemovePermission",
            "CreatePlatformApplication",
            "DeletePlatformApplication",
            "GetPlatformApplicationAttributes",
            "SetPlatformApplicationAttributes",
            "ListPlatformApplications",
            "CreatePlatformEndpoint",
            "DeleteEndpoint",
            "GetEndpointAttributes",
            "SetEndpointAttributes",
            "ListEndpointsByPlatformApplication",
            "SetSMSAttributes",
            "GetSMSAttributes",
            "CheckIfPhoneNumberIsOptedOut",
            "ListPhoneNumbersOptedOut",
            "OptInPhoneNumber",
        ]
    }
}

/// SNS uses Query protocol — params come from query_params (which includes form body).
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

/// Check if a topic ARN belongs to the given region
fn arn_region(arn: &str) -> Option<&str> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 4 {
        Some(parts[3])
    } else {
        None
    }
}

/// SNS uses XML responses for Query protocol.
fn xml_resp(inner: &str, _request_id: &str) -> AwsResponse {
    let xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{inner}\n");
    AwsResponse::xml(StatusCode::OK, xml)
}

const FIFO_NAME_ERROR: &str = "Fifo Topic names must end with .fifo and must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens, and must be between 1 and 256 characters long.";
const STANDARD_NAME_ERROR: &str = "Topic names must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens, and must be between 1 and 256 characters long.";

/// Validate a topic name according to AWS rules
fn validate_topic_name(name: &str, is_fifo_attr: bool) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 256 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            STANDARD_NAME_ERROR,
        ));
    }

    let base_name = name.strip_suffix(".fifo").unwrap_or(name);
    let valid_chars = base_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');

    if !valid_chars {
        let msg = if name.ends_with(".fifo") || is_fifo_attr {
            FIFO_NAME_ERROR
        } else {
            STANDARD_NAME_ERROR
        };
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            msg,
        ));
    }

    // FIFO validation
    if is_fifo_attr && !name.ends_with(".fifo") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            FIFO_NAME_ERROR,
        ));
    }

    if name.ends_with(".fifo") && !is_fifo_attr {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            STANDARD_NAME_ERROR,
        ));
    }

    Ok(())
}

impl SnsService {
    fn create_topic(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required(req, "Name")?;

        // Parse attributes from Attributes.entry.N.key / Attributes.entry.N.value
        let topic_attrs = parse_entries(req, "Attributes");
        let is_fifo_attr = topic_attrs
            .get("FifoTopic")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let is_fifo = name.ends_with(".fifo");

        validate_topic_name(&name, is_fifo_attr)?;

        // Parse tags from request
        let tags = parse_tags(req);

        let mut state = self.state.write();
        let topic_arn = format!("arn:aws:sns:{}:{}:{}", req.region, state.account_id, name);

        if !state.topics.contains_key(&topic_arn) {
            let mut attributes = HashMap::new();
            // Set default policy
            attributes.insert(
                "Policy".to_string(),
                default_policy(&topic_arn, &state.account_id),
            );
            attributes.insert("DisplayName".to_string(), String::new());
            attributes.insert("DeliveryPolicy".to_string(), String::new());

            if is_fifo {
                attributes.insert("FifoTopic".to_string(), "true".to_string());
                attributes.insert("ContentBasedDeduplication".to_string(), "false".to_string());
            }

            // Apply topic attributes from the request
            for (k, v) in &topic_attrs {
                // Normalize boolean-like values for FifoTopic and ContentBasedDeduplication
                if k == "FifoTopic" || k == "ContentBasedDeduplication" {
                    let normalized = if v.eq_ignore_ascii_case("true") {
                        "true"
                    } else {
                        "false"
                    };
                    if k == "FifoTopic" && normalized == "false" {
                        attributes.remove("FifoTopic");
                        attributes.remove("ContentBasedDeduplication");
                        continue;
                    }
                    attributes.insert(k.clone(), normalized.to_string());
                    continue;
                }
                attributes.insert(k.clone(), v.clone());
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

        // Filter topics by region
        let all_topics: Vec<&SnsTopic> = state
            .topics
            .values()
            .filter(|t| {
                // Extract region from ARN
                let parts: Vec<&str> = t.topic_arn.split(':').collect();
                parts.len() >= 4 && parts[3] == req.region
            })
            .collect();

        let next_token = param(req, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let next_token = next_token.min(all_topics.len());

        let page = &all_topics[next_token..];
        let has_more = page.len() > DEFAULT_PAGE_SIZE;
        let page = if has_more {
            &page[..DEFAULT_PAGE_SIZE]
        } else {
            page
        };

        let members: String = page
            .iter()
            .map(|t| {
                format!(
                    "      <member><TopicArn>{}</TopicArn></member>",
                    t.topic_arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let next_token_xml = if has_more {
            format!(
                "\n    <NextToken>{}</NextToken>",
                next_token + DEFAULT_PAGE_SIZE
            )
        } else {
            String::new()
        };

        Ok(xml_resp(
            &format!(
                r#"<ListTopicsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListTopicsResult>
    <Topics>
{members}
    </Topics>{next_token_xml}
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

        // Check region: topic must belong to the request's region
        if let Some(topic_region) = arn_region(&topic_arn) {
            if topic_region != req.region {
                return Err(not_found("Topic"));
            }
        }

        let state = self.state.read();
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

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

        let mut entries = vec![
            format_attr("TopicArn", &topic.topic_arn),
            format_attr("Owner", &state.account_id),
            format_attr("SubscriptionsConfirmed", &subs_confirmed.to_string()),
            format_attr("SubscriptionsPending", &subs_pending.to_string()),
            format_attr("SubscriptionsDeleted", "0"),
        ];

        // Add EffectiveDeliveryPolicy
        entries.push(format_attr(
            "EffectiveDeliveryPolicy",
            DEFAULT_EFFECTIVE_DELIVERY_POLICY,
        ));

        // Add all stored attributes
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

        // If setting Policy, compact the JSON
        if attr_name == "Policy" {
            if let Ok(parsed) = serde_json::from_str::<Value>(&attr_value) {
                topic
                    .attributes
                    .insert(attr_name, serde_json::to_string(&parsed).unwrap());
            } else {
                topic.attributes.insert(attr_name, attr_value);
            }
        } else {
            topic.attributes.insert(attr_name, attr_value);
        }

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
        let topic = state_r
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;
        let is_fifo_topic = topic.is_fifo;
        let account_id = state_r.account_id.clone();

        // Validate application endpoint exists
        if protocol == "application" {
            let endpoint_exists = state_r
                .platform_applications
                .values()
                .any(|app| app.endpoints.contains_key(&endpoint));
            if !endpoint_exists {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    format!(
                        "Invalid parameter: Endpoint Reason: Endpoint does not exist for endpoint arn {endpoint}"
                    ),
                ));
            }
        }
        drop(state_r);

        // Validate SMS endpoint
        if protocol == "sms" {
            validate_sms_endpoint(&endpoint)?;
        }

        // Validate SQS endpoint (must be an ARN)
        if protocol == "sqs" && !endpoint.starts_with("arn:aws:sqs:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: SQS endpoint ARN",
            ));
        }

        // Validate: FIFO SQS queues can only be subscribed to FIFO topics
        if protocol == "sqs" && endpoint.ends_with(".fifo") && !is_fifo_topic {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics",
            ));
        }

        // Parse subscription attributes
        let sub_attrs = parse_entries(req, "Attributes");

        // Validate subscription attribute names
        for key in sub_attrs.keys() {
            if !VALID_SUBSCRIPTION_ATTRS.contains(&key.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    format!("Invalid parameter: Attributes Reason: Unknown attribute: {key}"),
                ));
            }
        }

        // Validate and auto-set FilterPolicy
        let mut attributes = sub_attrs;
        if let Some(fp) = attributes.get("FilterPolicy") {
            if !fp.is_empty() {
                validate_filter_policy(fp)?;
            }
            if !attributes.contains_key("FilterPolicyScope") {
                attributes.insert(
                    "FilterPolicyScope".to_string(),
                    "MessageAttributes".to_string(),
                );
            }
        }

        // Check for duplicate subscription (same topic, protocol, endpoint)
        let mut state = self.state.write();
        for sub in state.subscriptions.values() {
            if sub.topic_arn == topic_arn && sub.protocol == protocol && sub.endpoint == endpoint {
                return Ok(xml_resp(
                    &format!(
                        r#"<SubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SubscribeResult>
    <SubscriptionArn>{}</SubscriptionArn>
  </SubscribeResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SubscribeResponse>"#,
                        sub.subscription_arn, req.request_id
                    ),
                    &req.request_id,
                ));
            }
        }

        let sub_arn = format!("{}:{}", topic_arn, uuid::Uuid::new_v4());

        let sub = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn,
            protocol,
            endpoint,
            owner: account_id,
            attributes,
            confirmed: true,
        };

        state.subscriptions.insert(sub_arn.clone(), sub);

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

        // Parse MessageAttributes from query params
        let message_attributes = parse_message_attributes(req);

        // Handle SMS publish (PhoneNumber)
        if let Some(ref phone) = phone_number {
            // Validate phone number (basic E.164: starts with + followed by digits)
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

            // SMS message length limit: 1600 characters
            if message.len() > 1600 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: Message Reason: Message must be less than 1600 characters long",
                ));
            }

            let msg_id = uuid::Uuid::new_v4().to_string();
            let mut state = self.state.write();
            state.sms_messages.push((phone.clone(), message.clone()));
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

            return Ok(xml_resp(
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
            ));
        }

        let topic_arn = topic_arn.unwrap();

        // Check if it's a platform endpoint ARN
        if topic_arn.contains(":endpoint/") {
            return self.publish_to_platform_endpoint(
                &topic_arn,
                &message,
                &message_attributes,
                &req.request_id,
            );
        }

        let mut state = self.state.write();
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        // FIFO topic enforcement
        if topic.is_fifo {
            if message_group_id.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: The request must contain the parameter MessageGroupId.",
                ));
            }
            // FIFO topics require deduplication: either ContentBasedDeduplication or explicit ID
            let content_dedup = topic
                .attributes
                .get("ContentBasedDeduplication")
                .map(|v| v == "true")
                .unwrap_or(false);
            if !content_dedup && message_dedup_id.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
        } else {
            // Non-FIFO: MessageGroupId is allowed (forwarded to SQS for fair queuing)
            // But DeduplicationId is NOT allowed on non-FIFO topics
            if message_dedup_id.is_some() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Invalid parameter: The request includes MessageDeduplicationId parameter that is not valid for this topic type",
                ));
            }
        }

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

        // Resolve the actual message per protocol for MessageStructure=json
        let parsed_structure: Option<Value> = if message_structure.as_deref() == Some("json") {
            serde_json::from_str(&message).ok()
        } else {
            None
        };

        // Collect subscribers
        let sqs_subscribers: Vec<(String, bool, Option<String>, Option<String>)> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.protocol == "sqs" && s.confirmed)
            .filter(|s| matches_filter_policy(s, &message_attributes, &message))
            .map(|s| {
                let raw = s
                    .attributes
                    .get("RawMessageDelivery")
                    .map(|v| v == "true")
                    .unwrap_or(false);
                (s.endpoint.clone(), raw, None, None)
            })
            .collect();

        let http_subscribers: Vec<String> = state
            .subscriptions
            .values()
            .filter(|s| {
                s.topic_arn == topic_arn
                    && (s.protocol == "http" || s.protocol == "https")
                    && s.confirmed
            })
            .filter(|s| matches_filter_policy(s, &message_attributes, &message))
            .map(|s| s.endpoint.clone())
            .collect();
        drop(state);

        // Determine actual message content per protocol
        let sqs_message = if let Some(ref structure) = parsed_structure {
            structure
                .get("sqs")
                .or_else(|| structure.get("default"))
                .and_then(|v| v.as_str())
                .unwrap_or(&message)
                .to_string()
        } else {
            message.clone()
        };

        let default_message = if let Some(ref structure) = parsed_structure {
            structure
                .get("default")
                .and_then(|v| v.as_str())
                .unwrap_or(&message)
                .to_string()
        } else {
            message.clone()
        };

        // Build SNS notification envelope
        let mut envelope_attrs = serde_json::Map::new();
        for (key, attr) in &message_attributes {
            let mut attr_obj = serde_json::Map::new();
            attr_obj.insert("Type".to_string(), Value::String(attr.data_type.clone()));
            if let Some(ref sv) = attr.string_value {
                attr_obj.insert("Value".to_string(), Value::String(sv.clone()));
            }
            if let Some(ref bv) = attr.binary_value {
                attr_obj.insert(
                    "Value".to_string(),
                    Value::String(base64::engine::general_purpose::STANDARD.encode(bv)),
                );
            }
            envelope_attrs.insert(key.clone(), Value::Object(attr_obj));
        }

        // Deliver to SQS subscribers
        for (queue_arn, raw, _msg_group, _msg_dedup) in &sqs_subscribers {
            if *raw {
                // Raw message delivery: just send the message body directly
                let mut sqs_msg_attrs = HashMap::new();
                for (k, v) in &message_attributes {
                    let mut attr = fakecloud_core::delivery::SqsMessageAttribute {
                        data_type: v.data_type.clone(),
                        string_value: v.string_value.clone(),
                        binary_value: None,
                    };
                    if let Some(ref bv) = v.binary_value {
                        attr.binary_value =
                            Some(base64::engine::general_purpose::STANDARD.encode(bv));
                    }
                    sqs_msg_attrs.insert(k.clone(), attr);
                }
                self.delivery.send_to_sqs_with_attrs(
                    queue_arn,
                    &sqs_message,
                    &sqs_msg_attrs,
                    message_group_id.as_deref(),
                    message_dedup_id.as_deref(),
                );
            } else {
                // Standard delivery: wrap in SNS envelope
                let envelope_str = build_sns_envelope(
                    &msg_id,
                    &topic_arn,
                    &subject,
                    &sqs_message,
                    &envelope_attrs,
                );
                self.delivery
                    .send_to_sqs(queue_arn, &envelope_str, &HashMap::new());
            }
        }

        // Deliver to HTTP/HTTPS subscribers (fire-and-forget)
        for endpoint_url in http_subscribers {
            let sns_envelope_str = build_sns_envelope(
                &msg_id,
                &topic_arn,
                &subject,
                &default_message,
                &envelope_attrs,
            );
            let body = sns_envelope_str;
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

    fn publish_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;

        let state = self.state.read();
        let topic = state
            .topics
            .get(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;
        let is_fifo = topic.is_fifo;
        drop(state);

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

        let mut successful = Vec::new();
        let failed: Vec<String> = Vec::new();

        for (idx, (id, message, subject, group_id, dedup_id, structure)) in
            entries.iter().enumerate()
        {
            // Parse per-entry message attributes
            let batch_attrs = parse_batch_message_attributes(req, idx + 1);

            let msg_id = uuid::Uuid::new_v4().to_string();
            let mut state = self.state.write();
            state.published.push(PublishedMessage {
                message_id: msg_id.clone(),
                topic_arn: topic_arn.clone(),
                message: message.clone(),
                subject: subject.clone(),
                message_attributes: batch_attrs.clone(),
                message_group_id: group_id.clone(),
                message_dedup_id: dedup_id.clone(),
                timestamp: Utc::now(),
            });

            // Resolve message for SQS via MessageStructure=json
            let parsed_structure: Option<Value> = if structure.as_deref() == Some("json") {
                serde_json::from_str(message).ok()
            } else {
                None
            };
            let sqs_message = if let Some(ref s) = parsed_structure {
                s.get("sqs")
                    .or_else(|| s.get("default"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(message)
                    .to_string()
            } else {
                message.clone()
            };

            // Deliver to SQS subscribers
            let sqs_subscribers: Vec<(String, bool)> = state
                .subscriptions
                .values()
                .filter(|s| s.topic_arn == topic_arn && s.protocol == "sqs" && s.confirmed)
                .map(|s| {
                    let raw = s
                        .attributes
                        .get("RawMessageDelivery")
                        .map(|v| v == "true")
                        .unwrap_or(false);
                    (s.endpoint.clone(), raw)
                })
                .collect();
            drop(state);

            // Build envelope attributes
            let mut envelope_attrs = serde_json::Map::new();
            for (key, attr) in &batch_attrs {
                let mut attr_obj = serde_json::Map::new();
                attr_obj.insert("Type".to_string(), Value::String(attr.data_type.clone()));
                if let Some(ref sv) = attr.string_value {
                    attr_obj.insert("Value".to_string(), Value::String(sv.clone()));
                }
                if let Some(ref bv) = attr.binary_value {
                    attr_obj.insert(
                        "Value".to_string(),
                        Value::String(base64::engine::general_purpose::STANDARD.encode(bv)),
                    );
                }
                envelope_attrs.insert(key.clone(), Value::Object(attr_obj));
            }

            for (queue_arn, raw) in &sqs_subscribers {
                if *raw {
                    let mut sqs_msg_attrs = HashMap::new();
                    for (k, v) in &batch_attrs {
                        let mut attr = fakecloud_core::delivery::SqsMessageAttribute {
                            data_type: v.data_type.clone(),
                            string_value: v.string_value.clone(),
                            binary_value: None,
                        };
                        if let Some(ref bv) = v.binary_value {
                            attr.binary_value =
                                Some(base64::engine::general_purpose::STANDARD.encode(bv));
                        }
                        sqs_msg_attrs.insert(k.clone(), attr);
                    }
                    self.delivery.send_to_sqs_with_attrs(
                        queue_arn,
                        &sqs_message,
                        &sqs_msg_attrs,
                        if is_fifo { group_id.as_deref() } else { None },
                        if is_fifo { dedup_id.as_deref() } else { None },
                    );
                } else {
                    let envelope_str = build_sns_envelope(
                        &msg_id,
                        &topic_arn,
                        subject,
                        &sqs_message,
                        &envelope_attrs,
                    );
                    self.delivery
                        .send_to_sqs(queue_arn, &envelope_str, &HashMap::new());
                }
            }

            successful.push(format!(
                r#"    <member>
      <Id>{id}</Id>
      <MessageId>{msg_id}</MessageId>
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

    fn publish_to_platform_endpoint(
        &self,
        endpoint_arn: &str,
        message: &str,
        message_attributes: &HashMap<String, MessageAttribute>,
        request_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

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
        drop(state);

        let msg_id = uuid::Uuid::new_v4().to_string();
        let mut state = self.state.write();
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

    fn list_subscriptions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let all_subs: Vec<&SnsSubscription> = state.subscriptions.values().collect();
        let next_token = param(req, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let next_token = next_token.min(all_subs.len());

        let page = &all_subs[next_token..];
        let has_more = page.len() > DEFAULT_PAGE_SIZE;
        let page = if has_more {
            &page[..DEFAULT_PAGE_SIZE]
        } else {
            page
        };

        let members: String = page
            .iter()
            .map(|s| format_sub_member(s))
            .collect::<Vec<_>>()
            .join("\n");

        let next_token_xml = if has_more {
            format!(
                "\n    <NextToken>{}</NextToken>",
                next_token + DEFAULT_PAGE_SIZE
            )
        } else {
            String::new()
        };

        Ok(xml_resp(
            &format!(
                r#"<ListSubscriptionsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsResult>
    <Subscriptions>
{members}
    </Subscriptions>{next_token_xml}
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

        let all_subs: Vec<&SnsSubscription> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn)
            .collect();

        let next_token = param(req, "NextToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let next_token = next_token.min(all_subs.len());

        let page = &all_subs[next_token..];
        let has_more = page.len() > DEFAULT_PAGE_SIZE;
        let page = if has_more {
            &page[..DEFAULT_PAGE_SIZE]
        } else {
            page
        };

        let members: String = page
            .iter()
            .map(|s| format_sub_member(s))
            .collect::<Vec<_>>()
            .join("\n");

        let next_token_xml = if has_more {
            format!(
                "\n    <NextToken>{}</NextToken>",
                next_token + DEFAULT_PAGE_SIZE
            )
        } else {
            String::new()
        };

        Ok(xml_resp(
            &format!(
                r#"<ListSubscriptionsByTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsByTopicResult>
    <Subscriptions>
{members}
    </Subscriptions>{next_token_xml}
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
            format_attr("Owner", &sub.owner),
            format_attr("ConfirmationWasAuthenticated", "true"),
            format_attr("PendingConfirmation", "false"),
        ];

        // Add RawMessageDelivery from attributes or default
        if !sub.attributes.contains_key("RawMessageDelivery") {
            entries.push(format_attr("RawMessageDelivery", "false"));
        }

        // Add EffectiveDeliveryPolicy
        entries.push(format_attr(
            "EffectiveDeliveryPolicy",
            DEFAULT_EFFECTIVE_DELIVERY_POLICY,
        ));

        for (k, v) in &sub.attributes {
            // Skip empty FilterPolicy (unsetting it removes it)
            if k == "FilterPolicy" && v.is_empty() {
                continue;
            }
            // If FilterPolicy is unset, also skip FilterPolicyScope
            if k == "FilterPolicyScope" {
                let has_filter = sub
                    .attributes
                    .get("FilterPolicy")
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);
                if !has_filter {
                    continue;
                }
            }
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

        // Validate attribute name
        if !VALID_SUBSCRIPTION_ATTRS.contains(&attr_name.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                "Invalid parameter: AttributeName".to_string(),
            ));
        }

        // Validate filter policy
        if attr_name == "FilterPolicy" && !attr_value.is_empty() {
            validate_filter_policy(&attr_value)?;
        }

        let mut state = self.state.write();
        let sub = state
            .subscriptions
            .get_mut(&sub_arn)
            .ok_or_else(|| not_found("Subscription"))?;

        sub.attributes.insert(attr_name.clone(), attr_value.clone());

        // Setting FilterPolicy auto-sets FilterPolicyScope
        if attr_name == "FilterPolicy" && !attr_value.is_empty() {
            sub.attributes
                .entry("FilterPolicyScope".to_string())
                .or_insert_with(|| "MessageAttributes".to_string());
        }

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
        let new_tags = parse_tags(req);

        let mut state = self.state.write();
        let topic = state.topics.get_mut(&resource_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                "Resource does not exist",
            )
        })?;

        // Check tag quota: existing + new (after dedup) must not exceed 50
        let mut merged = topic.tags.clone();
        for (k, v) in &new_tags {
            // Update existing or add
            if let Some(pos) = merged.iter().position(|(ek, _)| ek == k) {
                merged[pos] = (k.clone(), v.clone());
            } else {
                merged.push((k.clone(), v.clone()));
            }
        }
        if merged.len() > 50 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TagLimitExceeded",
                "Could not complete request: tag quota of per resource exceeded",
            ));
        }

        topic.tags = merged;

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
        let topic = state.topics.get_mut(&resource_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                "Resource does not exist",
            )
        })?;
        topic.tags.retain(|(k, _)| !tag_keys.contains(k));

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
        let topic = state.topics.get(&resource_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                "Resource does not exist",
            )
        })?;

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

    fn add_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let label = required(req, "Label")?;

        // Parse AWSAccountId.member.N and ActionName.member.N
        let mut account_ids = Vec::new();
        for n in 1..=20 {
            let key = format!("AWSAccountId.member.{n}");
            if let Some(v) = req.query_params.get(&key) {
                account_ids.push(v.clone());
            } else {
                break;
            }
        }

        let mut action_names = Vec::new();
        for n in 1..=20 {
            let key = format!("ActionName.member.{n}");
            if let Some(v) = req.query_params.get(&key) {
                action_names.push(v.clone());
            } else {
                break;
            }
        }

        // Validate action names
        for action in &action_names {
            if !VALID_SNS_ACTIONS.contains(&action.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameter",
                    "Policy statement action out of service scope!",
                ));
            }
        }

        let mut state = self.state.write();
        let account_id = state.account_id.clone();
        let topic = state
            .topics
            .get_mut(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        // Get or create policy
        let policy_str = topic
            .attributes
            .get("Policy")
            .cloned()
            .unwrap_or_else(|| default_policy(&topic_arn, &account_id));

        let mut policy: Value = serde_json::from_str(&policy_str).unwrap_or_else(|_| {
            serde_json::from_str(&default_policy(&topic_arn, &account_id)).unwrap()
        });

        // Check if statement with this label already exists
        if let Some(statements) = policy["Statement"].as_array() {
            for stmt in statements {
                if stmt["Sid"].as_str() == Some(&label) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        "Statement already exists",
                    ));
                }
            }
        }

        // Build principal
        let principal = if account_ids.len() == 1 {
            Value::String(format!("arn:aws:iam::{}:root", account_ids[0]))
        } else {
            Value::Array(
                account_ids
                    .iter()
                    .map(|id| Value::String(format!("arn:aws:iam::{}:root", id)))
                    .collect(),
            )
        };

        // Build action
        let action = if action_names.len() == 1 {
            Value::String(format!("SNS:{}", action_names[0]))
        } else {
            Value::Array(
                action_names
                    .iter()
                    .map(|a| Value::String(format!("SNS:{}", a)))
                    .collect(),
            )
        };

        let new_statement = serde_json::json!({
            "Sid": label,
            "Effect": "Allow",
            "Principal": {"AWS": principal},
            "Action": action,
            "Resource": topic_arn,
        });

        if let Some(statements) = policy["Statement"].as_array_mut() {
            statements.push(new_statement);
        }

        topic
            .attributes
            .insert("Policy".to_string(), policy.to_string());

        Ok(xml_resp(
            &format!(
                r#"<AddPermissionResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</AddPermissionResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn remove_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let topic_arn = required(req, "TopicArn")?;
        let label = required(req, "Label")?;

        let mut state = self.state.write();
        let topic = state
            .topics
            .get_mut(&topic_arn)
            .ok_or_else(|| not_found("Topic"))?;

        if let Some(policy_str) = topic.attributes.get("Policy").cloned() {
            if let Ok(mut policy) = serde_json::from_str::<Value>(&policy_str) {
                if let Some(statements) = policy["Statement"].as_array_mut() {
                    statements.retain(|s| s["Sid"].as_str() != Some(&label));
                }
                topic
                    .attributes
                    .insert("Policy".to_string(), policy.to_string());
            }
        }

        Ok(xml_resp(
            &format!(
                r#"<RemovePermissionResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</RemovePermissionResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    // ===== Platform Application actions =====

    fn create_platform_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(req, "Name")?;
        let platform = required(req, "Platform")?;
        let attributes = parse_entries(req, "Attributes");

        let mut state = self.state.write();
        let arn = format!(
            "arn:aws:sns:{}:{}:app/{}/{}",
            req.region, state.account_id, platform, name
        );

        state.platform_applications.insert(
            arn.clone(),
            PlatformApplication {
                arn: arn.clone(),
                name,
                platform,
                attributes,
                endpoints: HashMap::new(),
            },
        );

        Ok(xml_resp(
            &format!(
                r#"<CreatePlatformApplicationResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreatePlatformApplicationResult>
    <PlatformApplicationArn>{arn}</PlatformApplicationArn>
  </CreatePlatformApplicationResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePlatformApplicationResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn delete_platform_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required(req, "PlatformApplicationArn")?;
        self.state.write().platform_applications.remove(&arn);

        Ok(xml_resp(
            &format!(
                r#"<DeletePlatformApplicationResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeletePlatformApplicationResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_platform_application_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required(req, "PlatformApplicationArn")?;
        let state = self.state.read();
        let app = state
            .platform_applications
            .get(&arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        let attrs: String = app
            .attributes
            .iter()
            .map(|(k, v)| format_attr(k, v))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<GetPlatformApplicationAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetPlatformApplicationAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetPlatformApplicationAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetPlatformApplicationAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn set_platform_application_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required(req, "PlatformApplicationArn")?;
        let new_attrs = parse_entries(req, "Attributes");

        let mut state = self.state.write();
        let app = state
            .platform_applications
            .get_mut(&arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        for (k, v) in new_attrs {
            app.attributes.insert(k, v);
        }

        Ok(xml_resp(
            &format!(
                r#"<SetPlatformApplicationAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetPlatformApplicationAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_platform_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let members: String = state
            .platform_applications
            .values()
            .map(|app| {
                let attrs: String = app
                    .attributes
                    .iter()
                    .map(|(k, v)| format_attr(k, v))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    r#"      <member>
        <PlatformApplicationArn>{}</PlatformApplicationArn>
        <Attributes>
{attrs}
        </Attributes>
      </member>"#,
                    app.arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListPlatformApplicationsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListPlatformApplicationsResult>
    <PlatformApplications>
{members}
    </PlatformApplications>
  </ListPlatformApplicationsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListPlatformApplicationsResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn create_platform_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_arn = required(req, "PlatformApplicationArn")?;
        let token = required(req, "Token")?;
        let custom_user_data = param(req, "CustomUserData");
        let attrs = parse_entries(req, "Attributes");

        let mut state = self.state.write();
        let account_id = state.account_id.clone();
        let app = state
            .platform_applications
            .get_mut(&app_arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        // Check for existing endpoint with same token
        for (arn, ep) in &app.endpoints {
            if ep.token == token {
                // If attributes are different, check Enabled attribute
                let existing_enabled = ep
                    .attributes
                    .get("Enabled")
                    .cloned()
                    .unwrap_or_else(|| "true".to_string());
                let new_enabled = attrs
                    .get("Enabled")
                    .cloned()
                    .unwrap_or_else(|| "true".to_string());
                let custom_matches = match (&custom_user_data, ep.attributes.get("CustomUserData"))
                {
                    (Some(new), Some(old)) => new == old,
                    (None, None) => true,
                    (None, Some(_)) => true,
                    _ => false,
                };

                if existing_enabled == new_enabled && custom_matches {
                    // Return existing endpoint
                    return Ok(xml_resp(
                        &format!(
                            r#"<CreatePlatformEndpointResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreatePlatformEndpointResult>
    <EndpointArn>{arn}</EndpointArn>
  </CreatePlatformEndpointResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePlatformEndpointResponse>"#,
                            req.request_id
                        ),
                        &req.request_id,
                    ));
                } else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        format!("Invalid parameter: Token Reason: Endpoint {} already exists with the same Token, but different attributes.", arn),
                    ));
                }
            }
        }

        let endpoint_id = uuid::Uuid::new_v4().to_string().replace('-', "");
        let endpoint_arn = format!(
            "arn:aws:sns:{}:{}:endpoint/{}/{}/{}",
            req.region, account_id, app.platform, app.name, endpoint_id
        );

        let mut endpoint_attrs = attrs;
        endpoint_attrs
            .entry("Enabled".to_string())
            .or_insert_with(|| "true".to_string());
        endpoint_attrs.insert("Token".to_string(), token.clone());
        if let Some(ref ud) = custom_user_data {
            endpoint_attrs
                .entry("CustomUserData".to_string())
                .or_insert_with(|| ud.clone());
        }

        let enabled = endpoint_attrs
            .get("Enabled")
            .map(|v| v == "true")
            .unwrap_or(true);

        app.endpoints.insert(
            endpoint_arn.clone(),
            PlatformEndpoint {
                arn: endpoint_arn.clone(),
                token,
                attributes: endpoint_attrs,
                enabled,
                messages: Vec::new(),
            },
        );

        Ok(xml_resp(
            &format!(
                r#"<CreatePlatformEndpointResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreatePlatformEndpointResult>
    <EndpointArn>{endpoint_arn}</EndpointArn>
  </CreatePlatformEndpointResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePlatformEndpointResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn delete_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_arn = required(req, "EndpointArn")?;

        let mut state = self.state.write();
        for app in state.platform_applications.values_mut() {
            app.endpoints.remove(&endpoint_arn);
        }

        Ok(xml_resp(
            &format!(
                r#"<DeleteEndpointResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeleteEndpointResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_endpoint_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_arn = required(req, "EndpointArn")?;

        let state = self.state.read();
        for app in state.platform_applications.values() {
            if let Some(ep) = app.endpoints.get(&endpoint_arn) {
                let attrs: String = ep
                    .attributes
                    .iter()
                    .map(|(k, v)| format_attr(k, v))
                    .collect::<Vec<_>>()
                    .join("\n");

                return Ok(xml_resp(
                    &format!(
                        r#"<GetEndpointAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetEndpointAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetEndpointAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetEndpointAttributesResponse>"#,
                        req.request_id
                    ),
                    &req.request_id,
                ));
            }
        }

        Err(not_found("Endpoint"))
    }

    fn set_endpoint_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_arn = required(req, "EndpointArn")?;
        let new_attrs = parse_entries(req, "Attributes");

        let mut state = self.state.write();
        for app in state.platform_applications.values_mut() {
            if let Some(ep) = app.endpoints.get_mut(&endpoint_arn) {
                for (k, v) in new_attrs {
                    if k == "Enabled" {
                        ep.enabled = v == "true";
                    }
                    ep.attributes.insert(k, v);
                }

                return Ok(xml_resp(
                    &format!(
                        r#"<SetEndpointAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetEndpointAttributesResponse>"#,
                        req.request_id
                    ),
                    &req.request_id,
                ));
            }
        }

        Err(not_found("Endpoint"))
    }

    fn list_endpoints_by_platform_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_arn = required(req, "PlatformApplicationArn")?;

        let state = self.state.read();
        let app = state
            .platform_applications
            .get(&app_arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        let members: String = app
            .endpoints
            .values()
            .map(|ep| {
                let attrs: String = ep
                    .attributes
                    .iter()
                    .map(|(k, v)| format_attr(k, v))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    r#"      <member>
        <EndpointArn>{}</EndpointArn>
        <Attributes>
{attrs}
        </Attributes>
      </member>"#,
                    ep.arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListEndpointsByPlatformApplicationResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListEndpointsByPlatformApplicationResult>
    <Endpoints>
{members}
    </Endpoints>
  </ListEndpointsByPlatformApplicationResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListEndpointsByPlatformApplicationResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    // ===== SMS actions =====

    fn set_sms_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let attrs = parse_entries(req, "attributes");

        let mut state = self.state.write();
        for (k, v) in attrs {
            state.sms_attributes.insert(k, v);
        }

        Ok(xml_resp(
            &format!(
                r#"<SetSMSAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SetSMSAttributesResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetSMSAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn get_sms_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Parse optional attribute name filter: attributes.member.N
        let mut filter_names = Vec::new();
        for n in 1..=50 {
            let key = format!("attributes.member.{n}");
            if let Some(name) = req.query_params.get(&key) {
                filter_names.push(name.clone());
            } else {
                break;
            }
        }

        let state = self.state.read();

        let attrs: String = state
            .sms_attributes
            .iter()
            .filter(|(k, _)| filter_names.is_empty() || filter_names.contains(k))
            .map(|(k, v)| format_attr(k, v))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<GetSMSAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetSMSAttributesResult>
    <attributes>
{attrs}
    </attributes>
  </GetSMSAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSMSAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn check_if_phone_number_is_opted_out(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "phoneNumber")?;

        // Validate phone number format (E.164)
        let valid = phone_number.starts_with('+')
            && phone_number.len() >= 2
            && phone_number[1..].chars().all(|c| c.is_ascii_digit());
        if !valid {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: PhoneNumber Reason: {phone_number} does not meet the E164 format"
                ),
            ));
        }

        let state = self.state.read();
        // Numbers ending in 99 are considered opted out by convention
        let is_opted_out =
            state.opted_out_numbers.contains(&phone_number) || phone_number.ends_with("99");

        Ok(xml_resp(
            &format!(
                r#"<CheckIfPhoneNumberIsOptedOutResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CheckIfPhoneNumberIsOptedOutResult>
    <isOptedOut>{is_opted_out}</isOptedOut>
  </CheckIfPhoneNumberIsOptedOutResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CheckIfPhoneNumberIsOptedOutResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn list_phone_numbers_opted_out(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let members: String = state
            .opted_out_numbers
            .iter()
            .map(|n| format!("      <member>{n}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListPhoneNumbersOptedOutResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListPhoneNumbersOptedOutResult>
    <phoneNumbers>
{members}
    </phoneNumbers>
  </ListPhoneNumbersOptedOutResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListPhoneNumbersOptedOutResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    fn opt_in_phone_number(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "phoneNumber")?;
        let mut state = self.state.write();
        state.opted_out_numbers.retain(|n| n != &phone_number);

        Ok(xml_resp(
            &format!(
                r#"<OptInPhoneNumberResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <OptInPhoneNumberResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</OptInPhoneNumberResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }
}

/// Build an SNS notification envelope as JSON string.
/// Subject and MessageAttributes are only included when present.
fn build_sns_envelope(
    message_id: &str,
    topic_arn: &str,
    subject: &Option<String>,
    message: &str,
    message_attributes: &serde_json::Map<String, Value>,
) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "Type".to_string(),
        Value::String("Notification".to_string()),
    );
    map.insert(
        "MessageId".to_string(),
        Value::String(message_id.to_string()),
    );
    map.insert("TopicArn".to_string(), Value::String(topic_arn.to_string()));
    if let Some(ref subj) = subject {
        map.insert("Subject".to_string(), Value::String(subj.clone()));
    }
    map.insert("Message".to_string(), Value::String(message.to_string()));
    map.insert(
        "Timestamp".to_string(),
        Value::String(Utc::now().to_rfc3339()),
    );
    map.insert(
        "SignatureVersion".to_string(),
        Value::String("1".to_string()),
    );
    map.insert(
        "Signature".to_string(),
        Value::String("FAKE_SIGNATURE".to_string()),
    );
    map.insert(
        "SigningCertURL".to_string(),
        Value::String("https://sns.us-east-1.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem".to_string()),
    );
    map.insert(
        "UnsubscribeURL".to_string(),
        Value::String(format!(
            "http://localhost:4566/?Action=Unsubscribe&SubscriptionArn={}",
            topic_arn
        )),
    );
    if !message_attributes.is_empty() {
        map.insert(
            "MessageAttributes".to_string(),
            Value::Object(message_attributes.clone()),
        );
    }
    Value::Object(map).to_string()
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
        <Owner>{}</Owner>
      </member>"#,
        sub.subscription_arn, sub.topic_arn, sub.protocol, sub.endpoint, sub.owner,
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
            let binary_value_key = format!("MessageAttributes.entry.{n}.Value.BinaryValue");
            let binary_value = req
                .query_params
                .get(&binary_value_key)
                .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok());
            attrs.insert(
                name.clone(),
                MessageAttribute {
                    data_type: data_type.clone(),
                    string_value,
                    binary_value,
                },
            );
        } else {
            break;
        }
    }
    attrs
}

/// Parse MessageAttributes for a specific PublishBatch entry.
/// Format: PublishBatchRequestEntries.member.M.MessageAttributes.entry.N.Name/...
fn parse_batch_message_attributes(
    req: &AwsRequest,
    member_idx: usize,
) -> HashMap<String, MessageAttribute> {
    let mut attrs = HashMap::new();
    for n in 1..=10 {
        let prefix =
            format!("PublishBatchRequestEntries.member.{member_idx}.MessageAttributes.entry.{n}");
        let name_key = format!("{prefix}.Name");
        let data_type_key = format!("{prefix}.Value.DataType");
        if let (Some(name), Some(data_type)) = (
            req.query_params.get(&name_key),
            req.query_params.get(&data_type_key),
        ) {
            let sv_key = format!("{prefix}.Value.StringValue");
            let string_value = req.query_params.get(&sv_key).cloned();
            let bv_key = format!("{prefix}.Value.BinaryValue");
            let binary_value = req
                .query_params
                .get(&bv_key)
                .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok());
            attrs.insert(
                name.clone(),
                MessageAttribute {
                    data_type: data_type.clone(),
                    string_value,
                    binary_value,
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
fn parse_tags(req: &AwsRequest) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    for n in 1..=100 {
        let key_param = format!("Tags.member.{n}.Key");
        let val_param = format!("Tags.member.{n}.Value");
        if let Some(key) = req.query_params.get(&key_param) {
            let value = req
                .query_params
                .get(&val_param)
                .cloned()
                .unwrap_or_default();
            tags.push((key.clone(), value));
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

/// Parse Attributes.entry.N.key/value pairs (used by CreateTopic, Subscribe, etc.)
fn parse_entries(req: &AwsRequest, prefix: &str) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    for n in 1..=50 {
        let key_param = format!("{prefix}.entry.{n}.key");
        let val_param = format!("{prefix}.entry.{n}.value");
        if let Some(key) = req.query_params.get(&key_param) {
            let value = req
                .query_params
                .get(&val_param)
                .cloned()
                .unwrap_or_default();
            attrs.insert(key.clone(), value);
        } else {
            break;
        }
    }
    attrs
}

/// Validate SMS phone number
fn validate_sms_endpoint(endpoint: &str) -> Result<(), AwsServiceError> {
    // Allow formats like +15551234567 and +15/55-123.4567
    if endpoint.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Endpoint",
        ));
    }

    // Must start with optional + and contain only digits, -, /, .
    let stripped = endpoint.strip_prefix('+').unwrap_or(endpoint);
    if stripped.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("Invalid SMS endpoint: {endpoint}"),
        ));
    }

    // Check for invalid patterns: consecutive special chars, must start with + or digit
    if !endpoint.starts_with('+') && !endpoint.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("Invalid SMS endpoint: {endpoint}"),
        ));
    }

    // Must not end with a special char
    if endpoint.ends_with('.') || endpoint.ends_with('-') || endpoint.ends_with('/') {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("Invalid SMS endpoint: {endpoint}"),
        ));
    }

    // Must not have consecutive special chars like --
    let chars: Vec<char> = endpoint.chars().collect();
    for i in 0..chars.len() - 1 {
        let c = chars[i];
        let next = chars[i + 1];
        if (c == '-' || c == '/' || c == '.') && (next == '-' || next == '/' || next == '.') {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!("Invalid SMS endpoint: {endpoint}"),
            ));
        }
    }

    // Check all chars are valid
    for c in stripped.chars() {
        if !c.is_ascii_digit() && c != '-' && c != '/' && c != '.' {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!("Invalid SMS endpoint: {endpoint}"),
            ));
        }
    }

    Ok(())
}

/// Check if a message's attributes match the subscription's FilterPolicy.
fn matches_filter_policy(
    sub: &SnsSubscription,
    message_attributes: &HashMap<String, MessageAttribute>,
    message_body: &str,
) -> bool {
    let filter_json = match sub.attributes.get("FilterPolicy") {
        Some(fp) if !fp.is_empty() => fp,
        _ => return true,
    };

    let filter: HashMap<String, Value> = match serde_json::from_str(filter_json) {
        Ok(f) => f,
        Err(_) => return true,
    };

    let scope = sub
        .attributes
        .get("FilterPolicyScope")
        .map(|s| s.as_str())
        .unwrap_or("MessageAttributes");

    if scope == "MessageBody" {
        return matches_filter_policy_body(&filter, message_body);
    }

    // MessageAttributes scope
    for (attr_name, allowed_values) in &filter {
        // Handle $or operator
        if attr_name == "$or" {
            if let Some(or_conditions) = allowed_values.as_array() {
                let any_match = or_conditions.iter().any(|condition| {
                    if let Some(cond_obj) = condition.as_object() {
                        let cond_map: HashMap<String, Value> = cond_obj
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        // Each condition in $or is a mini filter policy
                        cond_map.iter().all(|(key, vals)| {
                            if let Some(arr) = vals.as_array() {
                                if let Some(msg_attr) = message_attributes.get(key) {
                                    let val = msg_attr.string_value.as_deref().unwrap_or("");
                                    check_filter_values(arr, val)
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        })
                    } else {
                        false
                    }
                });
                if !any_match {
                    return false;
                }
                continue;
            }
        }

        let allowed = match allowed_values.as_array() {
            Some(arr) => arr,
            None => continue,
        };

        let msg_attr = match message_attributes.get(attr_name) {
            Some(a) => a,
            None => {
                let has_exists_false = allowed.iter().any(|v| {
                    v.as_object()
                        .and_then(|o| o.get("exists"))
                        .and_then(|e| e.as_bool())
                        == Some(false)
                });
                if has_exists_false {
                    continue;
                }
                return false;
            }
        };

        let attr_value = msg_attr.string_value.as_deref().unwrap_or("");
        let is_numeric_type = msg_attr.data_type == "Number";

        // Handle String.Array data type: parse the JSON array and check each element
        if msg_attr.data_type.starts_with("String.Array") || msg_attr.data_type == "String.Array" {
            if let Ok(arr) = serde_json::from_str::<Vec<Value>>(attr_value) {
                let any_match = arr.iter().any(|elem| {
                    let elem_str = match elem {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        _ => elem.to_string(),
                    };
                    check_filter_values(allowed, &elem_str)
                });
                if !any_match {
                    return false;
                }
                continue;
            }
        }

        let matched = check_filter_values_typed(allowed, attr_value, Some(is_numeric_type));
        if !matched {
            return false;
        }
    }

    true
}

/// Match filter policy against message body (JSON)
fn matches_filter_policy_body(filter: &HashMap<String, Value>, message_body: &str) -> bool {
    let body: Value = match serde_json::from_str(message_body) {
        Ok(v) => v,
        Err(_) => return false,
    };

    matches_filter_policy_nested(filter, &body)
}

fn matches_filter_policy_nested(filter: &HashMap<String, Value>, body: &Value) -> bool {
    let body_obj = match body.as_object() {
        Some(o) => o,
        None => return false,
    };

    for (key, filter_value) in filter {
        let body_value = match body_obj.get(key) {
            Some(v) => v,
            None => {
                // Check for exists: false
                if let Some(arr) = filter_value.as_array() {
                    let has_exists_false = arr.iter().any(|v| {
                        v.as_object()
                            .and_then(|o| o.get("exists"))
                            .and_then(|e| e.as_bool())
                            == Some(false)
                    });
                    if has_exists_false {
                        continue;
                    }
                }
                return false;
            }
        };

        if let Some(arr) = filter_value.as_array() {
            // This is a leaf filter: check the value
            // If the body value is an array, check if ANY element matches
            if let Some(body_arr) = body_value.as_array() {
                let any_match = body_arr.iter().any(|elem| {
                    let is_elem_numeric = elem.is_number();
                    let elem_str = match elem {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => b.to_string(),
                        Value::Null => "null".to_string(),
                        _ => elem.to_string(),
                    };
                    check_filter_values_typed(arr, &elem_str, Some(is_elem_numeric))
                });
                if !any_match {
                    return false;
                }
            } else {
                let is_body_numeric = body_value.is_number();
                let value_str = match body_value {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "null".to_string(),
                    _ => body_value.to_string(),
                };
                if !check_filter_values_typed(arr, &value_str, Some(is_body_numeric)) {
                    return false;
                }
            }
        } else if let Some(nested_filter) = filter_value.as_object() {
            // Nested filter: recurse
            let nested_map: HashMap<String, Value> = nested_filter
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            // If body_value is an array, check if ANY element matches
            if let Some(body_arr) = body_value.as_array() {
                let any_match = body_arr
                    .iter()
                    .any(|elem| matches_filter_policy_nested(&nested_map, elem));
                if !any_match {
                    return false;
                }
            } else if !matches_filter_policy_nested(&nested_map, body_value) {
                return false;
            }
        }
    }

    true
}

/// Untyped filter check - used for String.Array elements, $or, and body array elements
/// where both string and numeric comparisons are allowed.
fn check_filter_values(allowed: &[Value], attr_value: &str) -> bool {
    check_filter_values_typed(allowed, attr_value, None)
}

/// Type-aware filter check. When `is_numeric` is Some(true), only Number filters match.
/// When Some(false), only String filters match. When None, both match (original behavior).
fn check_filter_values_typed(
    allowed: &[Value],
    attr_value: &str,
    is_numeric: Option<bool>,
) -> bool {
    allowed.iter().any(|v| match v {
        Value::String(s) => {
            // If we know the attribute is numeric, string filters don't match
            if is_numeric == Some(true) {
                false
            } else {
                s == attr_value
            }
        }
        Value::Number(n) => {
            // If we know the attribute is a string, number filters don't match
            if is_numeric == Some(false) {
                return false;
            }
            if let Ok(attr_num) = attr_value.parse::<f64>() {
                if let Some(filter_num) = n.as_f64() {
                    numbers_equal(attr_num, filter_num)
                } else {
                    false
                }
            } else {
                false
            }
        }
        Value::Bool(_) | Value::Null => false,
        Value::Object(obj) => {
            if let Some(prefix) = obj.get("prefix").and_then(|v| v.as_str()) {
                attr_value.starts_with(prefix)
            } else if let Some(suffix) = obj.get("suffix").and_then(|v| v.as_str()) {
                attr_value.ends_with(suffix)
            } else if let Some(anything_but) = obj.get("anything-but") {
                match anything_but {
                    Value::String(s) => {
                        // String anything-but only excludes string-type attrs
                        if is_numeric == Some(true) {
                            true
                        } else {
                            attr_value != s
                        }
                    }
                    Value::Number(n) => {
                        // Number anything-but only excludes number-type attrs
                        if is_numeric == Some(false) {
                            return true;
                        }
                        if let Ok(attr_num) = attr_value.parse::<f64>() {
                            if let Some(filter_num) = n.as_f64() {
                                (attr_num - filter_num).abs() >= f64::EPSILON
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    }
                    Value::Array(arr) => {
                        // anything-but with array: type must match for exclusion
                        !arr.iter().any(|av| match av {
                            Value::String(s) => {
                                if is_numeric == Some(true) {
                                    false
                                } else {
                                    s == attr_value
                                }
                            }
                            Value::Number(n) => {
                                if is_numeric == Some(false) {
                                    return false;
                                }
                                if let Ok(attr_num) = attr_value.parse::<f64>() {
                                    if let Some(filter_num) = n.as_f64() {
                                        numbers_equal(attr_num, filter_num)
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            }
                            _ => false,
                        })
                    }
                    Value::Object(inner) => {
                        // anything-but with prefix
                        if let Some(prefix) = inner.get("prefix").and_then(|v| v.as_str()) {
                            !attr_value.starts_with(prefix)
                        } else if let Some(suffix) = inner.get("suffix").and_then(|v| v.as_str()) {
                            !attr_value.ends_with(suffix)
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            } else if let Some(numeric_arr) = obj.get("numeric").and_then(|v| v.as_array()) {
                let attr_num: f64 = match attr_value.parse() {
                    Ok(n) => n,
                    Err(_) => return false,
                };
                matches_numeric_filter(attr_num, numeric_arr)
            } else if let Some(eq_ignore_case) =
                obj.get("equals-ignore-case").and_then(|v| v.as_str())
            {
                attr_value.eq_ignore_ascii_case(eq_ignore_case)
            } else {
                // {"exists": true/false}
                obj.get("exists")
                    .and_then(|v| v.as_bool())
                    .unwrap_or_default()
            }
        }
        _ => false,
    })
}

/// Compare two f64 values with limited precision (5 decimal places).
/// AWS SNS uses limited precision for number comparisons.
fn numbers_equal(a: f64, b: f64) -> bool {
    // Compare with ~5 decimal digit precision
    (a - b).abs() < 1e-5
}

/// Evaluate a numeric filter
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
            "=" => numbers_equal(value, threshold),
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

/// Validate a filter policy JSON string.
fn validate_filter_policy(policy_str: &str) -> Result<(), AwsServiceError> {
    let policy: HashMap<String, Value> = serde_json::from_str(policy_str).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: FilterPolicy: failed to parse JSON.",
        )
    })?;

    // Count total filter values across all keys (max 150)
    let mut total_values = 0;
    for (key, value) in &policy {
        // Skip special operators like $or
        if key.starts_with('$') {
            continue;
        }
        if let Some(arr) = value.as_array() {
            total_values += arr.len();
            for item in arr {
                validate_filter_policy_value(item)?;
            }
        }
    }
    if total_values > 150 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: FilterPolicy: Filter policy is too complex",
        ));
    }

    Ok(())
}

/// Known match type keys for filter policy objects.
const VALID_FILTER_MATCH_TYPES: &[&str] = &[
    "exists",
    "prefix",
    "suffix",
    "anything-but",
    "numeric",
    "equals-ignore-case",
];

/// Validate a single filter policy value.
fn validate_filter_policy_value(value: &Value) -> Result<(), AwsServiceError> {
    match value {
        Value::String(_) | Value::Bool(_) | Value::Null => Ok(()),
        Value::Number(n) => {
            // Number values must be within range
            if let Some(f) = n.as_f64() {
                if f.abs() >= 1_000_000_000.0 {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        format!(
                            "Invalid parameter: FilterPolicy: Match value {} must be smaller than 1E9",
                            n
                        ),
                    ));
                }
            }
            Ok(())
        }
        Value::Array(_) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: FilterPolicy: Match value must be String, number, true, false, or null",
        )),
        Value::Object(obj) => {
            if let Some(exists_val) = obj.get("exists") {
                if !exists_val.is_boolean() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        "Invalid parameter: FilterPolicy: exists match pattern must be either true or false.",
                    ));
                }
            }
            // Validate that object keys are recognized match types
            for key in obj.keys() {
                if !VALID_FILTER_MATCH_TYPES.contains(&key.as_str()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        format!(
                            "Invalid parameter: FilterPolicy: Unrecognized match type {key}"
                        ),
                    ));
                }
            }
            // Validate numeric filter operands
            if let Some(numeric_val) = obj.get("numeric") {
                if let Some(arr) = numeric_val.as_array() {
                    let mut i = 0;
                    while i < arr.len() {
                        if let Some(op) = arr[i].as_str() {
                            if i + 1 >= arr.len() {
                                break;
                            }
                            if !arr[i + 1].is_number() {
                                return Err(AwsServiceError::aws_error(
                                    StatusCode::BAD_REQUEST,
                                    "InvalidParameter",
                                    format!(
                                        "Invalid parameter: Attributes Reason: FilterPolicy: Value of {op} must be numeric\n at ..."
                                    ),
                                ));
                            }
                            i += 2;
                        } else {
                            break;
                        }
                    }
                }
            }
            Ok(())
        }
    }
}
