use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::DeliveryBus;

use super::{extract_xml_value, xml_escape};

pub(crate) fn normalize_notification_ids(xml: &str) -> String {
    let config_tags = [
        "TopicConfiguration",
        "QueueConfiguration",
        "CloudFunctionConfiguration",
        "LambdaFunctionConfiguration",
    ];
    let mut result = xml.to_string();
    for tag in &config_tags {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let mut output = String::new();
        let mut remaining = result.as_str();
        while let Some(start) = remaining.find(&open) {
            output.push_str(&remaining[..start]);
            let after = &remaining[start + open.len()..];
            if let Some(end) = after.find(&close) {
                let body = &after[..end];
                output.push_str(&open);
                if !body.contains("<Id>") {
                    output.push_str(&format!("<Id>{}</Id>", uuid::Uuid::new_v4()));
                }
                output.push_str(body);
                output.push_str(&close);
                remaining = &after[end + close.len()..];
            } else {
                output.push_str(&open);
                output.push_str(after);
                remaining = "";
                break;
            }
        }
        output.push_str(remaining);
        result = output;
    }
    result
}

pub(crate) fn normalize_replication_xml(xml: &str) -> String {
    let mut result = String::new();
    let mut remaining = xml;
    let mut auto_priority: u32 = 0;

    // Find and process everything before the first <Rule>
    if let Some(first_rule) = remaining.find("<Rule>") {
        result.push_str(&remaining[..first_rule]);
        remaining = &remaining[first_rule..];
    } else {
        return xml.to_string();
    }

    // Process each <Rule>
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        if let Some(rule_end) = after.find("</Rule>") {
            let rule_body = &after[..rule_end];

            // Extract fields from the rule
            let id = extract_xml_value(rule_body, "ID");
            let priority = extract_xml_value(rule_body, "Priority");
            let status =
                extract_xml_value(rule_body, "Status").unwrap_or_else(|| "Enabled".to_string());

            // Extract Destination block (keep as-is)
            let destination = rule_body.find("<Destination>").and_then(|ds| {
                rule_body
                    .find("</Destination>")
                    .map(|de| rule_body[ds..de + 14].to_string())
            });

            // Extract existing Filter if any
            let filter_block = rule_body.find("<Filter>").and_then(|fs| {
                rule_body
                    .find("</Filter>")
                    .map(|fe| rule_body[fs..fe + 9].to_string())
            });

            // Extract DeleteMarkerReplication if any
            let dmr_block = rule_body.find("<DeleteMarkerReplication>").and_then(|ds| {
                rule_body
                    .find("</DeleteMarkerReplication>")
                    .map(|de| rule_body[ds..de + 25].to_string())
            });

            // Build normalized rule
            result.push_str("<Rule>");

            // DeleteMarkerReplication (default to Disabled)
            result.push_str(dmr_block.as_deref().unwrap_or(
                "<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>",
            ));

            // Destination
            if let Some(ref dest) = destination {
                result.push_str(dest);
            }

            // Filter (default to empty prefix)
            result.push_str(
                filter_block
                    .as_deref()
                    .unwrap_or("<Filter><Prefix></Prefix></Filter>"),
            );

            // ID (auto-generate if missing)
            let rule_id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            result.push_str(&format!("<ID>{}</ID>", xml_escape(&rule_id)));

            // Priority (auto-assign if missing)
            auto_priority += 1;
            let p = priority
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(auto_priority);
            result.push_str(&format!("<Priority>{p}</Priority>"));

            // Status
            result.push_str(&format!("<Status>{status}</Status>"));

            result.push_str("</Rule>");

            remaining = &after[rule_end + 7..];
        } else {
            result.push_str(&remaining[rule_start..]);
            break;
        }
    }

    // Append anything after the last </Rule>
    result.push_str(remaining);

    result
}

/// Parsed replication rule extracted from the replication config XML.
pub(crate) struct ReplicationRule {
    pub(crate) status: String,
    pub(crate) prefix: String,
    pub(crate) dest_bucket: String,
}

/// Parse replication configuration XML and extract rules.
pub(crate) fn parse_replication_rules(xml: &str) -> Vec<ReplicationRule> {
    let mut rules = Vec::new();
    let mut remaining = xml;
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        if let Some(rule_end) = after.find("</Rule>") {
            let rule_body = &after[..rule_end];

            // Extract the rule-level Status. Skip Status tags inside nested
            // elements like DeleteMarkerReplication by finding the last occurrence.
            let status = {
                let mut found = None;
                let mut search = rule_body;
                while let Some(pos) = search.find("<Status>") {
                    if let Some(val) = extract_xml_value(&search[pos..], "Status") {
                        found = Some(val);
                    }
                    search = &search[pos + 8..];
                }
                found.unwrap_or_else(|| "Enabled".to_string())
            };

            // Extract prefix from Filter > Prefix or top-level Prefix
            let prefix = rule_body
                .find("<Filter>")
                .and_then(|fs| rule_body.find("</Filter>").map(|fe| &rule_body[fs..fe + 9]))
                .and_then(|filter| extract_xml_value(filter, "Prefix"))
                .or_else(|| extract_xml_value(rule_body, "Prefix"))
                .unwrap_or_default();

            // Extract destination bucket ARN and convert to bucket name
            let dest_bucket = rule_body
                .find("<Destination>")
                .and_then(|ds| {
                    rule_body
                        .find("</Destination>")
                        .map(|de| &rule_body[ds..de + 14])
                })
                .and_then(|dest| extract_xml_value(dest, "Bucket"))
                .map(|arn| {
                    // ARN format: arn:aws:s3:::bucket-name
                    arn.rsplit(":::").next().unwrap_or(&arn).to_string()
                })
                .unwrap_or_default();

            if !dest_bucket.is_empty() {
                rules.push(ReplicationRule {
                    status,
                    prefix,
                    dest_bucket,
                });
            }

            remaining = &after[rule_end + 7..];
        } else {
            break;
        }
    }
    rules
}

/// Replicate an object to destination buckets based on replication configuration.
/// Called after storing an object in a bucket that has replication enabled.
pub(crate) fn replicate_object(state: &mut crate::state::S3State, source_bucket: &str, key: &str) {
    let replication_config = match state.buckets.get(source_bucket) {
        Some(b) => match &b.replication_config {
            Some(config) => config.clone(),
            None => return,
        },
        None => return,
    };

    let rules = parse_replication_rules(&replication_config);
    let src_obj = match state
        .buckets
        .get(source_bucket)
        .and_then(|b| b.objects.get(key))
    {
        Some(obj) => obj.clone(),
        None => return,
    };

    for rule in &rules {
        if rule.status != "Enabled" {
            continue;
        }
        if !key.starts_with(&rule.prefix) {
            continue;
        }
        if let Some(dest_bucket) = state.buckets.get_mut(&rule.dest_bucket) {
            let mut replica = src_obj.clone();
            replica.storage_class = "STANDARD".to_string();
            // Use a new version ID if destination has versioning enabled
            if dest_bucket.versioning.as_deref() == Some("Enabled") {
                let vid = uuid::Uuid::new_v4().to_string();
                replica.version_id = Some(vid);
                dest_bucket
                    .object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(replica.clone());
            } else {
                replica.version_id = None;
            }
            dest_bucket.objects.insert(key.to_string(), replica);
        }
    }
}

/// Build an S3 event notification JSON payload.
pub(crate) fn build_s3_event_notification(
    event_name: &str,
    bucket_name: &str,
    key: &str,
    size: u64,
    etag: &str,
    region: &str,
) -> String {
    let event_time = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    serde_json::json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": region,
            "eventTime": event_time,
            "eventName": event_name,
            "s3": {
                "bucket": {
                    "name": bucket_name,
                    "arn": format!("arn:aws:s3:::{}", bucket_name)
                },
                "object": {
                    "key": key,
                    "size": size,
                    "eTag": etag
                }
            }
        }]
    })
    .to_string()
}

/// Parsed notification target from the bucket notification config XML.
pub(crate) struct NotificationTarget {
    pub(crate) target_type: NotificationTargetType,
    pub(crate) arn: String,
    pub(crate) events: Vec<String>,
    pub(crate) prefix_filter: Option<String>,
    pub(crate) suffix_filter: Option<String>,
}

pub(crate) enum NotificationTargetType {
    Sqs,
    Sns,
    Lambda,
}

/// Parse S3Key filter rules (prefix/suffix) from a notification configuration block.
pub(crate) fn parse_s3_key_filters(block: &str) -> (Option<String>, Option<String>) {
    let mut prefix = None;
    let mut suffix = None;
    if let Some(filter_start) = block.find("<Filter>") {
        let after_filter = &block[filter_start..];
        if let Some(filter_end) = after_filter.find("</Filter>") {
            let filter_block = &after_filter[..filter_end];
            // Parse each FilterRule
            let mut remaining = filter_block;
            while let Some(rule_start) = remaining.find("<FilterRule>") {
                let after_rule = &remaining[rule_start + 12..];
                if let Some(rule_end) = after_rule.find("</FilterRule>") {
                    let rule_block = &after_rule[..rule_end];
                    let name = extract_xml_value(rule_block, "Name");
                    let value = extract_xml_value(rule_block, "Value");
                    if let (Some(name), Some(value)) = (name, value) {
                        match name.to_lowercase().as_str() {
                            "prefix" => prefix = Some(value),
                            "suffix" => suffix = Some(value),
                            _ => {}
                        }
                    }
                    remaining = &after_rule[rule_end + 13..];
                } else {
                    break;
                }
            }
        }
    }
    (prefix, suffix)
}

/// Check if an object key matches the prefix/suffix filters.
pub(crate) fn key_matches_filters(
    key: &str,
    prefix: &Option<String>,
    suffix: &Option<String>,
) -> bool {
    if let Some(p) = prefix {
        if !key.starts_with(p.as_str()) {
            return false;
        }
    }
    if let Some(s) = suffix {
        if !key.ends_with(s.as_str()) {
            return false;
        }
    }
    true
}

/// Parse the bucket notification configuration XML into targets.
pub(crate) fn parse_notification_config(xml: &str) -> Vec<NotificationTarget> {
    let mut targets = Vec::new();

    // Parse QueueConfiguration entries
    let mut remaining = xml;
    while let Some(start) = remaining.find("<QueueConfiguration>") {
        let after = &remaining[start + 20..];
        if let Some(end) = after.find("</QueueConfiguration>") {
            let block = &after[..end];
            if let Some(arn) = extract_xml_value(block, "Queue") {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Sqs,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 21..];
        } else {
            break;
        }
    }

    // Parse TopicConfiguration entries
    remaining = xml;
    while let Some(start) = remaining.find("<TopicConfiguration>") {
        let after = &remaining[start + 20..];
        if let Some(end) = after.find("</TopicConfiguration>") {
            let block = &after[..end];
            if let Some(arn) = extract_xml_value(block, "Topic") {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Sns,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 21..];
        } else {
            break;
        }
    }

    // Parse CloudFunctionConfiguration entries (older S3 XML format)
    remaining = xml;
    while let Some(start) = remaining.find("<CloudFunctionConfiguration>") {
        let after = &remaining[start + 28..];
        if let Some(end) = after.find("</CloudFunctionConfiguration>") {
            let block = &after[..end];
            if let Some(arn) = extract_xml_value(block, "CloudFunction") {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Lambda,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 29..];
        } else {
            break;
        }
    }

    // Parse LambdaFunctionConfiguration entries (newer S3 XML format)
    remaining = xml;
    while let Some(start) = remaining.find("<LambdaFunctionConfiguration>") {
        let after = &remaining[start + 29..];
        if let Some(end) = after.find("</LambdaFunctionConfiguration>") {
            let block = &after[..end];
            // The newer format uses <Function> for the ARN
            let arn = extract_xml_value(block, "Function")
                .or_else(|| extract_xml_value(block, "CloudFunction"));
            if let Some(arn) = arn {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Lambda,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 30..];
        } else {
            break;
        }
    }

    targets
}

/// Extract all values for a given XML tag (multiple occurrences).
pub(crate) fn extract_all_xml_values(xml: &str, tag: &str) -> Vec<String> {
    let mut values = Vec::new();
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut remaining = xml;
    while let Some(start) = remaining.find(&open) {
        let after = &remaining[start + open.len()..];
        if let Some(end) = after.find(&close) {
            values.push(after[..end].to_string());
            remaining = &after[end + close.len()..];
        } else {
            break;
        }
    }
    values
}

/// Check if an S3 event name matches a notification event filter.
pub(crate) fn event_matches(event_name: &str, filter: &str) -> bool {
    // Exact match
    if filter == event_name {
        return true;
    }
    // Wildcard: s3:ObjectCreated:* matches s3:ObjectCreated:Put, etc.
    if filter.ends_with(":*") {
        let prefix = &filter[..filter.len() - 1]; // "s3:ObjectCreated:"
        if event_name.starts_with(prefix) {
            return true;
        }
    }
    // s3:* matches everything
    if filter == "s3:*" {
        return true;
    }
    false
}

/// Deliver S3 event notifications for a bucket operation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_notifications(
    delivery: &Arc<DeliveryBus>,
    notification_config: &str,
    event_name: &str,
    bucket_name: &str,
    key: &str,
    size: u64,
    etag: &str,
    region: &str,
) {
    let targets = parse_notification_config(notification_config);
    let s3_event_name = format!("s3:{event_name}");
    let message = build_s3_event_notification(event_name, bucket_name, key, size, etag, region);

    for target in &targets {
        let matches = target.events.is_empty()
            || target
                .events
                .iter()
                .any(|f| event_matches(&s3_event_name, f));
        if !matches {
            continue;
        }
        if !key_matches_filters(key, &target.prefix_filter, &target.suffix_filter) {
            continue;
        }
        match target.target_type {
            NotificationTargetType::Sqs => {
                delivery.send_to_sqs(&target.arn, &message, &std::collections::HashMap::new());
            }
            NotificationTargetType::Sns => {
                delivery.publish_to_sns(&target.arn, &message, Some("Amazon S3 Notification"));
            }
            NotificationTargetType::Lambda => {
                let delivery = delivery.clone();
                let function_arn = target.arn.clone();
                let payload = message.clone();
                tokio::spawn(async move {
                    tracing::info!(
                        function_arn = %function_arn,
                        "S3 invoking Lambda function for notification"
                    );
                    match delivery.invoke_lambda(&function_arn, &payload).await {
                        Some(Ok(_)) => {
                            tracing::info!(
                                function_arn = %function_arn,
                                "S3->Lambda invocation succeeded"
                            );
                        }
                        Some(Err(e)) => {
                            tracing::error!(
                                function_arn = %function_arn,
                                error = %e,
                                "S3->Lambda invocation failed"
                            );
                        }
                        None => {
                            tracing::warn!(
                                function_arn = %function_arn,
                                "No Lambda delivery configured"
                            );
                        }
                    }
                });
            }
        }
    }
}
