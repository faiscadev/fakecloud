use super::*;
use fakecloud_aws::xml::xml_escape;

/// Actions that mutate SNS state.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateTopic"
            | "DeleteTopic"
            | "SetTopicAttributes"
            | "Subscribe"
            | "ConfirmSubscription"
            | "Unsubscribe"
            | "Publish"
            | "PublishBatch"
            | "SetSubscriptionAttributes"
            | "TagResource"
            | "UntagResource"
            | "AddPermission"
            | "RemovePermission"
            | "CreatePlatformApplication"
            | "DeletePlatformApplication"
            | "SetPlatformApplicationAttributes"
            | "CreatePlatformEndpoint"
            | "DeleteEndpoint"
            | "SetEndpointAttributes"
            | "SetSMSAttributes"
            | "OptInPhoneNumber"
            | "CreateSMSSandboxPhoneNumber"
            | "DeleteSMSSandboxPhoneNumber"
            | "VerifySMSSandboxPhoneNumber"
            | "PutDataProtectionPolicy"
            // ListOriginationNumbers lazy-seeds a default origination number
            // on first read; treat it as mutating so the snapshot stays
            // consistent across restarts.
            | "ListOriginationNumbers"
    )
}

pub(crate) fn default_policy(topic_arn: &str, account_id: &str) -> String {
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

/// SNS uses Query protocol — params come from query_params (which includes form body).
pub(crate) fn param(req: &AwsRequest, name: &str) -> Option<String> {
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

pub(crate) fn required(req: &AwsRequest, name: &str) -> Result<String, AwsServiceError> {
    param(req, name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("The request must contain the parameter {name}"),
        )
    })
}

/// Validate MaxResults is in [min, max] per Smithy `@range` traits.
///
/// AWS SNS rejects out-of-range MaxResults with `InvalidParameter` (HTTP 400)
/// rather than silently clamping. Different list ops use different ranges
/// (e.g. ListOriginationNumbers 1..=30, ListSMSSandboxPhoneNumbers 1..=100).
pub(crate) fn validate_max_results(
    req: &AwsRequest,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    let Some(raw) = param(req, "MaxResults") else {
        return Ok(());
    };
    let n: i64 = raw.trim().parse().map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("Invalid parameter: MaxResults must be an integer, got {raw}"),
        )
    })?;
    if n < min || n > max {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("Invalid parameter: MaxResults must be between {min} and {max}, got {n}"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_message_structure_json(message: &str) -> Result<(), AwsServiceError> {
    let parsed: Value = serde_json::from_str(message).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Message Structure - No JSON message body is parseable",
        )
    })?;
    if parsed.get("default").is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Message Structure - No default entry in JSON message body",
        ));
    }
    Ok(())
}

/// Lowercase hex SHA-256 of the input, used for SNS FIFO
/// content-based deduplication (1.4).
pub(crate) fn sns_sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub(crate) fn not_found(entity: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NotFound",
        format!("{entity} does not exist"),
    )
}

/// Check if a topic ARN belongs to the given region
pub(crate) fn arn_region(arn: &str) -> Option<&str> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 4 {
        Some(parts[3])
    } else {
        None
    }
}

/// SNS uses XML responses for Query protocol.
pub(crate) fn xml_resp(inner: &str, _request_id: &str) -> AwsResponse {
    let xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{inner}\n");
    AwsResponse::xml(StatusCode::OK, xml)
}

/// Validate a topic name according to AWS rules
pub(crate) fn validate_topic_name(name: &str, is_fifo_attr: bool) -> Result<(), AwsServiceError> {
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

pub(crate) fn is_valid_sandbox_language(code: &str) -> bool {
    SUPPORTED_SANDBOX_LANGUAGES.contains(&code)
}

pub(crate) fn validate_e164(phone: &str) -> Result<(), AwsServiceError> {
    // E.164: leading '+' followed by country code + subscriber digits.
    // Real range is up to 15 digits with at least 4 (smallest standard
    // country plus subscriber). AWS rejects shorter numbers like "+1".
    let digits = phone.strip_prefix('+').unwrap_or("");
    let digit_only = digits.chars().all(|c| c.is_ascii_digit());
    let valid = phone.starts_with('+') && digit_only && (4..=15).contains(&digits.len());
    if !valid {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!("Invalid parameter: PhoneNumber Reason: {phone} does not meet the E164 format"),
        ));
    }
    Ok(())
}

pub(crate) fn rand_u32() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    nanos.wrapping_mul(2654435761)
}

/// Build an SNS Lambda event payload (matches real AWS format).
/// Used by both direct Publish and cross-service delivery.
pub(crate) fn build_sns_lambda_event(input: &SnsLambdaEventInput<'_>) -> String {
    let timestamp = input.timestamp.to_rfc3339();
    // AWS Lambda SNS event always emits the Subject key (empty when unset).
    // Pass the same value to the canonical builder so the signature covers
    // exactly what verifiers will reconstruct from the JSON.
    let subject_for_envelope = input.subject.unwrap_or("");
    let canonical = crate::signing::canonical_notification(
        input.message,
        input.message_id,
        Some(subject_for_envelope),
        &timestamp,
        input.topic_arn,
    );
    let signature = crate::signing::sign(&canonical);
    let sns_event = serde_json::json!({
        "Records": [{
            "EventVersion": "1.0",
            "EventSubscriptionArn": input.subscription_arn,
            "EventSource": "aws:sns",
            "Sns": {
                // SignatureVersion 2 corresponds to RSA-SHA256 signatures; v1 was SHA-1.
                "SignatureVersion": "2",
                "Timestamp": timestamp,
                "Signature": signature,
                "SigningCertUrl": crate::signing::cert_url(input.endpoint),
                "MessageId": input.message_id,
                "Message": input.message,
                "MessageAttributes": input.message_attributes,
                "Type": "Notification",
                "UnsubscribeUrl": format!("{}/?Action=Unsubscribe&SubscriptionArn={}", input.endpoint, input.subscription_arn),
                "TopicArn": input.topic_arn,
                "Subject": subject_for_envelope,
            }
        }]
    });
    sns_event.to_string()
}

/// Build an SNS SubscriptionConfirmation envelope as a JSON string.
/// AWS POSTs this to HTTP/HTTPS endpoints right after Subscribe so the
/// subscriber can echo the Token back via ConfirmSubscription. The
/// `server_endpoint` is the public URL of this fakecloud instance and
/// is used to build a SubscribeURL the subscriber can actually GET to
/// confirm — pointing at sns.amazonaws.com would defeat the purpose of
/// running locally.
pub(crate) fn build_subscription_confirmation_envelope(
    topic_arn: &str,
    token: &str,
    server_endpoint: &str,
) -> String {
    // Compute every field once, sign over those exact values, then insert.
    // Re-deriving the uuid/timestamp for the signature (the original bug) would
    // sign a canonical string the verifier can never reconstruct.
    let message_id = uuid::Uuid::new_v4().to_string();
    let message = format!(
        "You have chosen to subscribe to the topic {topic_arn}.\nTo confirm the subscription, visit the SubscribeURL included in this message."
    );
    let base = server_endpoint.trim_end_matches('/');
    let subscribe_url =
        format!("{base}/?Action=ConfirmSubscription&TopicArn={topic_arn}&Token={token}");
    let timestamp = Utc::now().to_rfc3339();
    let canonical = crate::signing::canonical_subscription_confirmation(
        &message,
        &message_id,
        &subscribe_url,
        &timestamp,
        token,
        topic_arn,
        "SubscriptionConfirmation",
    );
    let signature = crate::signing::sign(&canonical);

    let mut map = serde_json::Map::new();
    map.insert(
        "Type".to_string(),
        Value::String("SubscriptionConfirmation".to_string()),
    );
    map.insert("MessageId".to_string(), Value::String(message_id));
    map.insert("Token".to_string(), Value::String(token.to_string()));
    map.insert("TopicArn".to_string(), Value::String(topic_arn.to_string()));
    map.insert("Message".to_string(), Value::String(message));
    map.insert("SubscribeURL".to_string(), Value::String(subscribe_url));
    map.insert("Timestamp".to_string(), Value::String(timestamp));
    // SignatureVersion 2 corresponds to RSA-SHA256 signatures; v1 was SHA-1.
    map.insert(
        "SignatureVersion".to_string(),
        Value::String("2".to_string()),
    );
    map.insert(
        "SigningCertURL".to_string(),
        Value::String(crate::signing::cert_url(server_endpoint)),
    );
    map.insert("Signature".to_string(), Value::String(signature));
    Value::Object(map).to_string()
}

/// Generate a 256-character alphanumeric token for SNS subscription
/// confirmation. AWS issues opaque base64 tokens of similar length;
/// matching the shape keeps client libraries that validate token
/// length happy.
pub(crate) fn generate_confirmation_token() -> String {
    use rand::distributions::{Alphanumeric, DistString};
    Alphanumeric.sample_string(&mut rand::thread_rng(), 256)
}

/// Build an SNS notification envelope as JSON string.
/// Subject and MessageAttributes are only included when present.
pub(crate) fn build_sns_envelope(
    message_id: &str,
    topic_arn: &str,
    subscription_arn: &str,
    subject: &Option<String>,
    message: &str,
    message_attributes: &serde_json::Map<String, Value>,
    endpoint: &str,
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
    let timestamp = Utc::now().to_rfc3339();
    let canonical = crate::signing::canonical_notification(
        message,
        message_id,
        subject.as_deref(),
        &timestamp,
        topic_arn,
    );
    let signature = crate::signing::sign(&canonical);
    map.insert("Timestamp".to_string(), Value::String(timestamp));
    // SignatureVersion 2 corresponds to RSA-SHA256 signatures; v1 was SHA-1.
    map.insert(
        "SignatureVersion".to_string(),
        Value::String("2".to_string()),
    );
    map.insert("Signature".to_string(), Value::String(signature));
    map.insert(
        "SigningCertURL".to_string(),
        Value::String(crate::signing::cert_url(endpoint)),
    );
    map.insert(
        "UnsubscribeURL".to_string(),
        Value::String(format!(
            "{}/?Action=Unsubscribe&SubscriptionArn={}",
            endpoint, subscription_arn
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

pub(crate) fn format_attr(name: &str, value: &str) -> String {
    // Both the attribute name and value can carry user-controlled data
    // (e.g. Endpoint URLs with query strings, DisplayName, DeliveryPolicy).
    // Escape both so the emitted XML stays well-formed for client SDK parsers.
    format!(
        "      <entry><key>{}</key><value>{}</value></entry>",
        xml_escape(name),
        xml_escape(value)
    )
}

pub(crate) fn format_sub_member(sub: &SnsSubscription) -> String {
    let display_arn = if sub.confirmed {
        &sub.subscription_arn
    } else {
        "PendingConfirmation"
    };
    format!(
        r#"      <member>
        <SubscriptionArn>{}</SubscriptionArn>
        <TopicArn>{}</TopicArn>
        <Protocol>{}</Protocol>
        <Endpoint>{}</Endpoint>
        <Owner>{}</Owner>
      </member>"#,
        xml_escape(display_arn),
        xml_escape(&sub.topic_arn),
        xml_escape(&sub.protocol),
        xml_escape(&sub.endpoint),
        xml_escape(&sub.owner),
    )
}

pub(crate) fn collect_topic_subscribers(
    state: &crate::state::SnsState,
    topic_arn: &str,
    message_attributes: &BTreeMap<String, MessageAttribute>,
    message: &str,
) -> TopicSubscribers {
    let confirmed_for_topic = |s: &&SnsSubscription| {
        s.topic_arn == topic_arn
            && s.confirmed
            && matches_filter_policy(s, message_attributes, message)
    };

    let sqs = state
        .subscriptions
        .values()
        .filter(|s| s.protocol == "sqs")
        .filter(confirmed_for_topic)
        .map(|s| {
            let raw = s
                .attributes
                .get("RawMessageDelivery")
                .map(|v| v == "true")
                .unwrap_or(false);
            (
                s.endpoint.clone(),
                raw,
                s.subscription_arn.clone(),
                s.attributes.get("RedrivePolicy").cloned(),
            )
        })
        .collect();

    let http = state
        .subscriptions
        .values()
        .filter(|s| s.protocol == "http" || s.protocol == "https")
        .filter(confirmed_for_topic)
        .map(|s| crate::service::HttpSubscriber {
            endpoint: s.endpoint.clone(),
            subscription_arn: s.subscription_arn.clone(),
            protocol: s.protocol.clone(),
            delivery_policy: s.attributes.get("DeliveryPolicy").cloned(),
            redrive_policy: s.attributes.get("RedrivePolicy").cloned(),
        })
        .collect();

    let lambda = state
        .subscriptions
        .values()
        .filter(|s| s.protocol == "lambda")
        .filter(confirmed_for_topic)
        .map(|s| {
            (
                s.endpoint.clone(),
                s.subscription_arn.clone(),
                s.attributes.get("RedrivePolicy").cloned(),
            )
        })
        .collect();

    let email = state
        .subscriptions
        .values()
        .filter(|s| s.protocol == "email" || s.protocol == "email-json")
        .filter(confirmed_for_topic)
        .map(|s| (s.endpoint.clone(), s.protocol.clone()))
        .collect();

    let sms = state
        .subscriptions
        .values()
        .filter(|s| s.protocol == "sms")
        .filter(confirmed_for_topic)
        .map(|s| s.endpoint.clone())
        .collect();

    TopicSubscribers {
        sqs,
        http,
        lambda,
        email,
        sms,
    }
}

/// Build the `MessageAttributes` object used inside an SNS notification
/// envelope from the typed SNS message attributes.
pub(crate) fn build_envelope_attrs(
    message_attributes: &BTreeMap<String, MessageAttribute>,
) -> serde_json::Map<String, Value> {
    let mut envelope_attrs = serde_json::Map::new();
    for (key, attr) in message_attributes {
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
    envelope_attrs
}

pub(crate) fn parse_message_attributes(req: &AwsRequest) -> BTreeMap<String, MessageAttribute> {
    let mut attrs = BTreeMap::new();
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
pub(crate) fn parse_batch_message_attributes(
    req: &AwsRequest,
    member_idx: usize,
) -> BTreeMap<String, MessageAttribute> {
    let mut attrs = BTreeMap::new();
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
pub(crate) fn parse_tags(req: &AwsRequest) -> Vec<(String, String)> {
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
pub(crate) fn parse_tag_keys(req: &AwsRequest) -> Vec<String> {
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
pub(crate) fn parse_entries(req: &AwsRequest, prefix: &str) -> BTreeMap<String, String> {
    let mut attrs = BTreeMap::new();
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
pub(crate) fn validate_sms_endpoint(endpoint: &str) -> Result<(), AwsServiceError> {
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
pub(crate) fn matches_filter_policy(
    sub: &SnsSubscription,
    message_attributes: &BTreeMap<String, MessageAttribute>,
    message_body: &str,
) -> bool {
    let filter_json = match sub.attributes.get("FilterPolicy") {
        Some(fp) if !fp.is_empty() => fp,
        _ => return true,
    };

    let filter: HashMap<String, Value> = match serde_json::from_str(filter_json) {
        Ok(f) => f,
        Err(_) => return false,
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
                                    // Type-aware, matching the non-$or path: a
                                    // numeric filter must not match a string
                                    // attribute (and vice versa).
                                    let is_numeric_type = msg_attr.data_type == "Number";
                                    check_filter_values_typed(arr, val, Some(is_numeric_type))
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
pub(crate) fn matches_filter_policy_body(
    filter: &HashMap<String, Value>,
    message_body: &str,
) -> bool {
    let body: Value = match serde_json::from_str(message_body) {
        Ok(v) => v,
        Err(_) => return false,
    };

    matches_filter_policy_nested(filter, &body)
}

pub(crate) fn matches_filter_policy_nested(filter: &HashMap<String, Value>, body: &Value) -> bool {
    let body_obj = match body.as_object() {
        Some(o) => o,
        None => return false,
    };

    for (key, filter_value) in filter {
        // `$or`: the body matches when ANY branch (itself a full sub-policy)
        // matches. AWS supports this in the MessageBody scope; the body-scope
        // matcher previously treated "$or" as a literal field name, found it
        // absent, and dropped the subscriber.
        if key == "$or" {
            if let Some(branches) = filter_value.as_array() {
                let any = branches.iter().any(|branch| {
                    branch
                        .as_object()
                        .map(|o| {
                            let sub: HashMap<String, Value> =
                                o.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            matches_filter_policy_nested(&sub, body)
                        })
                        .unwrap_or(false)
                });
                if !any {
                    return false;
                }
                continue;
            }
        }

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
pub(crate) fn check_filter_values(allowed: &[Value], attr_value: &str) -> bool {
    check_filter_values_typed(allowed, attr_value, None)
}

/// Type-aware filter check. When `is_numeric` is Some(true), only Number filters match.
/// When Some(false), only String filters match. When None, both match (original behavior).
pub(crate) fn check_filter_values_typed(
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
                                // Exclude when the value equals the filter number
                                // using the same limited precision (~1e-5) that
                                // every other numeric comparison uses, so a
                                // single-value anything-but is consistent with the
                                // array form, `=`, and numeric ranges.
                                !numbers_equal(attr_num, filter_num)
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
pub(crate) fn numbers_equal(a: f64, b: f64) -> bool {
    // Compare with ~5 decimal digit precision
    (a - b).abs() < 1e-5
}

/// Evaluate a numeric filter
pub(crate) fn matches_numeric_filter(value: f64, conditions: &[Value]) -> bool {
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
pub(crate) fn validate_filter_policy(policy_str: &str) -> Result<(), AwsServiceError> {
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

/// Validate a single filter policy value.
pub(crate) fn validate_filter_policy_value(value: &Value) -> Result<(), AwsServiceError> {
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
                    validate_numeric_filter(arr)?;
                }
            }
            Ok(())
        }
    }
}

pub(crate) fn validate_numeric_filter(arr: &[Value]) -> Result<(), AwsServiceError> {
    // Empty array
    if arr.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Attributes Reason: FilterPolicy: Invalid member in numeric match: ]\n at ...",
        ));
    }

    // First element must be a string operator
    let first_op = match arr[0].as_str() {
        Some(s) => s,
        None => {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: Attributes Reason: FilterPolicy: Invalid member in numeric match: {}\n at ...",
                    arr[0]
                ),
            ));
        }
    };

    // Must be a recognized operator
    if !VALID_NUMERIC_OPS.contains(&first_op) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!(
                "Invalid parameter: Attributes Reason: FilterPolicy: Unrecognized numeric range operator: {first_op}\n at ..."
            ),
        ));
    }

    // Must have a value after the operator
    if arr.len() < 2 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!(
                "Invalid parameter: Attributes Reason: FilterPolicy: Value of {first_op} must be numeric\n at ..."
            ),
        ));
    }

    // Value must be numeric
    if !arr[1].is_number() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!(
                "Invalid parameter: Attributes Reason: FilterPolicy: Value of {first_op} must be numeric\n at ..."
            ),
        ));
    }

    // Numeric operand must be smaller than 1E9
    if let Some(f) = arr[1].as_f64() {
        if f.abs() >= 1_000_000_000.0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: FilterPolicy: Numeric match value must be smaller than 1E9, got {}",
                    arr[1]
                ),
            ));
        }
    }

    // Single comparison (2 elements): valid
    if arr.len() == 2 {
        return Ok(());
    }

    // Range expression: must have exactly 4 elements
    if arr.len() > 4 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Attributes Reason: FilterPolicy: Too many elements in numeric expression\n at ...",
        ));
    }

    if arr.len() < 4 {
        // 3 elements: op, val, op_missing_value
        if let Some(op2) = arr[2].as_str() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: Attributes Reason: FilterPolicy: Value of {op2} must be numeric\n at ..."
                ),
            ));
        }
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Attributes Reason: FilterPolicy: Too many elements in numeric expression\n at ...",
        ));
    }

    // Exactly 4 elements: range expression
    let second_op = match arr[2].as_str() {
        Some(s) => s,
        None => {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: Attributes Reason: FilterPolicy: Invalid member in numeric match: {}\n at ...",
                    arr[2]
                ),
            ));
        }
    };

    if !arr[3].is_number() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!(
                "Invalid parameter: Attributes Reason: FilterPolicy: Value of {second_op} must be numeric\n at ..."
            ),
        ));
    }

    // Numeric operand must be smaller than 1E9
    if let Some(f) = arr[3].as_f64() {
        if f.abs() >= 1_000_000_000.0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: FilterPolicy: Numeric match value must be smaller than 1E9, got {}",
                    arr[3]
                ),
            ));
        }
    }

    // For a range, first op must be lower bound (> or >=) and second op must be upper bound (< or <=)
    let first_is_lower = LOWER_OPS.contains(&first_op);
    let second_is_upper = UPPER_OPS.contains(&second_op);

    if first_is_lower && !second_is_upper {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            format!(
                "Invalid parameter: Attributes Reason: FilterPolicy: Bad numeric range operator: {second_op}\n at ..."
            ),
        ));
    }

    if !first_is_lower {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Attributes Reason: FilterPolicy: Too many elements in numeric expression\n at ...",
        ));
    }

    // Bottom must be less than top
    let bottom = arr[1].as_f64().unwrap_or(0.0);
    let top = arr[3].as_f64().unwrap_or(0.0);
    if bottom >= top {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "Invalid parameter: Attributes Reason: FilterPolicy: Bottom must be less than top\n at ...",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod confirmation_envelope_tests {
    use super::{build_subscription_confirmation_envelope, generate_confirmation_token};
    use serde_json::Value;

    #[test]
    fn envelope_carries_token_and_subscribe_url() {
        let topic = "arn:aws:sns:us-east-1:123456789012:test-topic";
        let token = "tok-abc";
        let server = "http://localhost:4566";
        let body = build_subscription_confirmation_envelope(topic, token, server);
        let parsed: Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(parsed["Type"], "SubscriptionConfirmation");
        assert_eq!(parsed["TopicArn"], topic);
        assert_eq!(parsed["Token"], token);
        let url = parsed["SubscribeURL"].as_str().unwrap();
        assert!(url.starts_with(server));
        assert!(url.contains("Action=ConfirmSubscription"));
        assert!(url.contains(token));
        assert!(url.contains(topic));
        assert!(parsed["Timestamp"].is_string());
        assert!(parsed["MessageId"].is_string());
        // SHA-256 signatures are SignatureVersion 2; the signer is RSA-SHA256.
        assert_eq!(parsed["SignatureVersion"], "2");
        assert!(!parsed["Signature"].as_str().unwrap().is_empty());
        assert_eq!(
            parsed["SigningCertURL"].as_str().unwrap(),
            "http://localhost:4566/_fakecloud/sns/cert.pem"
        );
    }

    /// The Signature must actually cover the envelope: rebuild the AWS
    /// canonical string from the envelope's own fields and verify it against
    /// the public key in the signing cert. This is the guarantee real
    /// `sns_message_validator` libraries rely on, and the assertion whose
    /// absence let the broken canonical string ship.
    #[test]
    fn signature_verifies_over_envelope_fields() {
        use base64::Engine;
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        use rsa::pkcs8::DecodePublicKey;
        use rsa::sha2::Sha256;
        use rsa::signature::Verifier;
        use rsa::RsaPublicKey;
        use x509_cert::der::{DecodePem, Encode};
        use x509_cert::Certificate;

        let topic = "arn:aws:sns:us-east-1:123456789012:verify-topic";
        let token = "tok-verify";
        let server = "http://localhost:4566";
        let body = build_subscription_confirmation_envelope(topic, token, server);
        let parsed: Value = serde_json::from_str(&body).expect("valid JSON");

        let canonical = crate::signing::canonical_subscription_confirmation(
            parsed["Message"].as_str().unwrap(),
            parsed["MessageId"].as_str().unwrap(),
            parsed["SubscribeURL"].as_str().unwrap(),
            parsed["Timestamp"].as_str().unwrap(),
            parsed["Token"].as_str().unwrap(),
            parsed["TopicArn"].as_str().unwrap(),
            parsed["Type"].as_str().unwrap(),
        );

        let cert = Certificate::from_pem(crate::signing::cert_pem().as_bytes()).unwrap();
        let spki_der = cert
            .tbs_certificate
            .subject_public_key_info
            .to_der()
            .unwrap();
        let public_key = RsaPublicKey::from_public_key_der(&spki_der).unwrap();
        let verifying = VerifyingKey::<Sha256>::new(public_key);

        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(parsed["Signature"].as_str().unwrap())
            .unwrap();
        let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
        verifying
            .verify(canonical.as_bytes(), &signature)
            .expect("confirmation signature must verify against the signing cert");
    }

    #[test]
    fn envelope_trims_trailing_slash() {
        let body = build_subscription_confirmation_envelope(
            "arn:aws:sns:us-east-1:123456789012:t",
            "tok",
            "http://localhost:4566/",
        );
        let parsed: Value = serde_json::from_str(&body).expect("valid JSON");
        let url = parsed["SubscribeURL"].as_str().unwrap();
        assert!(!url.contains("//?"), "must not double-up the slash: {url}");
    }

    #[test]
    fn confirmation_token_is_256_alphanumeric() {
        let t = generate_confirmation_token();
        assert_eq!(t.len(), 256);
        assert!(t.chars().all(|c| c.is_ascii_alphanumeric()));
        // Two calls should differ.
        let t2 = generate_confirmation_token();
        assert_ne!(t, t2);
    }

    #[test]
    fn body_scope_filter_supports_or() {
        use std::collections::HashMap;
        let policy: HashMap<String, Value> = serde_json::from_str(
            r#"{"$or":[{"price":[{"numeric":[">",100]}]},{"category":["urgent"]}]}"#,
        )
        .unwrap();

        // Matches the second branch.
        assert!(super::matches_filter_policy_body(
            &policy,
            r#"{"category":"urgent"}"#
        ));
        // Matches the first branch.
        assert!(super::matches_filter_policy_body(
            &policy,
            r#"{"price":150}"#
        ));
        // Matches neither branch.
        assert!(!super::matches_filter_policy_body(
            &policy,
            r#"{"category":"normal","price":10}"#
        ));
    }
}

#[cfg(test)]
mod xml_escape_render_tests {
    use super::{format_attr, format_sub_member};
    use crate::state::SnsSubscription;
    use std::collections::BTreeMap;

    #[test]
    fn format_attr_escapes_endpoint_query_string() {
        // The most common reachable trigger: an HTTPS endpoint with a query
        // string. The raw `&` must become `&amp;` or the client XML parser
        // hard-errors on ListSubscriptions/GetSubscriptionAttributes.
        let out = format_attr("Endpoint", "https://example.com/hook?token=a&env=prod");
        assert!(out.contains("token=a&amp;env=prod"), "got: {out}");
        assert!(!out.contains("a&env"), "raw ampersand leaked: {out}");
    }

    #[test]
    fn format_attr_escapes_display_name_and_angle_brackets() {
        let out = format_attr("DisplayName", "Acme & Co <inc>");
        assert!(out.contains("Acme &amp; Co &lt;inc&gt;"), "got: {out}");
    }

    #[test]
    fn format_sub_member_escapes_endpoint() {
        let sub = SnsSubscription {
            subscription_arn: "arn:aws:sns:us-east-1:123456789012:t:sub".to_string(),
            topic_arn: "arn:aws:sns:us-east-1:123456789012:t".to_string(),
            protocol: "https".to_string(),
            endpoint: "https://example.com/hook?a=1&b=2".to_string(),
            owner: "123456789012".to_string(),
            attributes: BTreeMap::new(),
            confirmed: true,
            confirmation_token: None,
        };
        let out = format_sub_member(&sub);
        assert!(out.contains("<Endpoint>https://example.com/hook?a=1&amp;b=2</Endpoint>"));
        assert!(!out.contains("a=1&b=2"), "raw ampersand leaked: {out}");
    }
}

#[cfg(test)]
mod filter_consistency_tests {
    use super::{check_filter_values_typed, matches_filter_policy};
    use crate::state::{MessageAttribute, SnsSubscription};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn num_attr(v: &str) -> MessageAttribute {
        MessageAttribute {
            data_type: "Number".to_string(),
            string_value: Some(v.to_string()),
            binary_value: None,
        }
    }

    fn str_attr(v: &str) -> MessageAttribute {
        MessageAttribute {
            data_type: "String".to_string(),
            string_value: Some(v.to_string()),
            binary_value: None,
        }
    }

    fn sub_with_filter(policy: serde_json::Value) -> SnsSubscription {
        let mut attributes = BTreeMap::new();
        attributes.insert("FilterPolicy".to_string(), policy.to_string());
        SnsSubscription {
            subscription_arn: "arn:aws:sns:us-east-1:123456789012:t:sub".to_string(),
            topic_arn: "arn:aws:sns:us-east-1:123456789012:t".to_string(),
            protocol: "sqs".to_string(),
            endpoint: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
            owner: "123456789012".to_string(),
            attributes,
            confirmed: true,
            confirmation_token: None,
        }
    }

    #[test]
    fn single_value_anything_but_uses_limited_precision() {
        // A value within ~1e-5 of the filter number is "equal" and must be
        // excluded, consistent with `=`, ranges, and array anything-but.
        let filter = vec![json!({"anything-but": 100})];
        // 100 is equal -> excluded -> no match.
        assert!(!check_filter_values_typed(&filter, "100", Some(true)));
        // 100.0000001 is within tolerance -> still treated as equal -> excluded.
        assert!(!check_filter_values_typed(
            &filter,
            "100.0000001",
            Some(true)
        ));
        // 101 differs -> not excluded -> matches.
        assert!(check_filter_values_typed(&filter, "101", Some(true)));
    }

    #[test]
    fn or_branch_is_type_aware() {
        // A numeric filter inside $or must NOT match a string attribute.
        let policy = json!({ "$or": [ { "price": [100] } ] });
        let sub = sub_with_filter(policy);

        let mut numeric_attrs = BTreeMap::new();
        numeric_attrs.insert("price".to_string(), num_attr("100"));
        assert!(matches_filter_policy(&sub, &numeric_attrs, ""));

        // Same textual value but the attribute is a String -> no match.
        let mut string_attrs = BTreeMap::new();
        string_attrs.insert("price".to_string(), str_attr("100"));
        assert!(!matches_filter_policy(&sub, &string_attrs, ""));
    }
}
