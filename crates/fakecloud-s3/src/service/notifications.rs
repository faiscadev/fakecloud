use std::sync::Arc;

use chrono::Utc;

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;

use crate::state::{S3NotificationEvent, SharedS3State};

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

            // Extract Destination block (keep as-is). The open/close tags are
            // located independently, so a body with the closing tag before the
            // opening one would slice with begin > end and panic (dropping the
            // connection -- a reachable DoS). Guard each slice so a malformed
            // ordering is skipped instead of crashing.
            let destination = rule_body.find("<Destination>").and_then(|ds| {
                rule_body
                    .find("</Destination>")
                    .filter(|&de| de >= ds)
                    .map(|de| rule_body[ds..de + 14].to_string())
            });

            // Extract existing Filter if any
            let filter_block = rule_body.find("<Filter>").and_then(|fs| {
                rule_body
                    .find("</Filter>")
                    .filter(|&fe| fe >= fs)
                    .map(|fe| rule_body[fs..fe + 9].to_string())
            });

            // Extract DeleteMarkerReplication if any
            let dmr_block = rule_body.find("<DeleteMarkerReplication>").and_then(|ds| {
                rule_body
                    .find("</DeleteMarkerReplication>")
                    .filter(|&de| de >= ds)
                    .map(|de| rule_body[ds..de + "</DeleteMarkerReplication>".len()].to_string())
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
/// Kept for tests only; production paths use [`replicate_through_store`].
#[cfg(test)]
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

/// Replicate an object to destination buckets AND persist the replica through
/// the S3 store so disk-mode restarts see it. Called from PutObject/CopyObject
/// write paths. Replaces the in-memory BodyRef of each replica with the one
/// returned by `put_object` (Disk in persistent mode, Memory otherwise).
pub(crate) fn replicate_through_store(
    state: &mut crate::state::S3State,
    store: &std::sync::Arc<dyn fakecloud_persistence::S3Store>,
    source_bucket: &str,
    key: &str,
) -> fakecloud_persistence::StoreResult<()> {
    let replication_config = match state.buckets.get(source_bucket) {
        Some(b) => match &b.replication_config {
            Some(config) => config.clone(),
            None => return Ok(()),
        },
        None => return Ok(()),
    };

    let rules = parse_replication_rules(&replication_config);
    let src_obj = match state
        .buckets
        .get(source_bucket)
        .and_then(|b| b.objects.get(key))
    {
        Some(obj) => obj.clone(),
        None => return Ok(()),
    };

    // For disk-backed sources, hold only the path and stream the source file
    // directly to each replica via FileCopy. For memory sources, read once.
    let src_disk_path: Option<std::path::PathBuf> = match &src_obj.body {
        fakecloud_persistence::BodyRef::Disk { path, .. } => Some(path.clone()),
        fakecloud_persistence::BodyRef::Memory(_) => None,
    };
    let src_bytes_opt: Option<bytes::Bytes> = if src_disk_path.is_none() {
        Some(
            state
                .read_body(&src_obj.body)
                .map_err(fakecloud_persistence::StoreError::Io)?,
        )
    } else {
        None
    };

    for rule in &rules {
        if rule.status != "Enabled" {
            continue;
        }
        if !key.starts_with(&rule.prefix) {
            continue;
        }
        let dest_bucket_name = rule.dest_bucket.clone();
        let dest_versioning_enabled;
        let (dest_version_id, dest_meta) = {
            let Some(dest_bucket) = state.buckets.get_mut(&dest_bucket_name) else {
                continue;
            };
            dest_versioning_enabled = dest_bucket.versioning.as_deref() == Some("Enabled");
            let mut replica = src_obj.clone();
            replica.storage_class = "STANDARD".to_string();
            // Seed the runtime replica body from whatever we have handy; it
            // is overwritten after `put_object` returns the canonical ref.
            let seed_body = match (&src_disk_path, &src_bytes_opt) {
                (Some(_), _) => src_obj.body.clone(),
                (None, Some(b)) => crate::state::memory_body(b.clone()),
                (None, None) => src_obj.body.clone(),
            };
            if dest_versioning_enabled {
                let vid = uuid::Uuid::new_v4().to_string();
                replica.version_id = Some(vid.clone());
                replica.body = seed_body;
                dest_bucket
                    .object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(replica.clone());
                dest_bucket.objects.insert(key.to_string(), replica.clone());
                (
                    Some(vid),
                    crate::persistence::object_meta_snapshot(&replica),
                )
            } else {
                replica.version_id = None;
                replica.body = seed_body;
                dest_bucket.objects.insert(key.to_string(), replica.clone());
                (None, crate::persistence::object_meta_snapshot(&replica))
            }
        };

        let body_source = match (&src_disk_path, &src_bytes_opt) {
            (Some(path), _) => fakecloud_persistence::BodySource::FileCopy(path.clone()),
            (None, Some(b)) => fakecloud_persistence::BodySource::Bytes(b.clone()),
            (None, None) => fakecloud_persistence::BodySource::Bytes(bytes::Bytes::new()),
        };
        let returned = store.put_object(
            &dest_bucket_name,
            key,
            dest_version_id.as_deref(),
            body_source,
            &dest_meta,
        )?;
        if let Some(dest_bucket) = state.buckets.get_mut(&dest_bucket_name) {
            if let Some(o) = dest_bucket.objects.get_mut(key) {
                o.body = returned.clone();
            }
            // Only rewrite the version-history entry when the destination
            // bucket actually has versioning enabled. For Suspended or
            // unversioned buckets the replica was only stored as the current
            // object; rewriting stale history would corrupt it.
            if dest_versioning_enabled {
                if let Some(versions) = dest_bucket.object_versions.get_mut(key) {
                    if let Some(last) = versions.last_mut() {
                        last.body = returned;
                    }
                }
            }
        }
    }
    Ok(())
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
                    "arn": Arn::s3(bucket_name).to_string()
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

/// Everything a reader needs to describe a single S3 object-level event.
pub(crate) struct ObjectEvent<'a> {
    pub event_name: &'a str,
    pub bucket_name: &'a str,
    pub key: &'a str,
    pub size: u64,
    pub etag: &'a str,
    pub region: &'a str,
}

/// Deliver S3 event notifications for a bucket operation.
pub(crate) fn deliver_notifications(
    delivery: &Arc<DeliveryBus>,
    notification_config: &str,
    event: &ObjectEvent<'_>,
    s3_state: Option<&SharedS3State>,
) {
    let ObjectEvent {
        event_name,
        bucket_name,
        key,
        size,
        etag,
        region,
    } = *event;

    let targets = parse_notification_config(notification_config);
    let s3_event_name = format!("s3:{event_name}");
    let message = build_s3_event_notification(event_name, bucket_name, key, size, etag, region);

    // Deliver to EventBridge if enabled for this bucket
    let eventbridge_enabled = s3_state
        .and_then(|st| {
            let mas = st.read();
            let acct = mas.find_account(|s| s.buckets.contains_key(bucket_name))?;
            mas.get(acct)
                .and_then(|s| s.buckets.get(bucket_name))
                .map(|b| b.eventbridge_enabled)
        })
        .unwrap_or(false);
    if eventbridge_enabled {
        let detail = serde_json::json!({
            "version": "0",
            "bucket": { "name": bucket_name },
            "object": { "key": key, "size": size, "etag": etag },
            "request-id": uuid::Uuid::new_v4().to_string(),
            "requester": "123456789012",
        });
        delivery.put_event_to_eventbridge(
            "aws.s3",
            &format!("Object {event_name}"),
            &detail.to_string(),
            "default",
        );
    }

    let mut delivered = false;

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
        delivered = true;
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

    // Record notification event for introspection only if at least one target matched
    if delivered {
        if let Some(state) = s3_state {
            let mut mas = state.write();
            let owner_acct = mas
                .find_account(|s| s.buckets.contains_key(bucket_name))
                .map(|a| a.to_string());
            if let Some(acct) = owner_acct {
                if let Some(acct_state) = mas.get_mut(&acct) {
                    acct_state.notification_events.push(S3NotificationEvent {
                        bucket: bucket_name.to_string(),
                        key: key.to_string(),
                        event_type: s3_event_name,
                        timestamp: Utc::now(),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_matches_exact() {
        assert!(event_matches(
            "s3:ObjectCreated:Put",
            "s3:ObjectCreated:Put"
        ));
        assert!(!event_matches(
            "s3:ObjectCreated:Put",
            "s3:ObjectCreated:Post"
        ));
    }

    #[test]
    fn event_matches_suffix_wildcard() {
        assert!(event_matches("s3:ObjectCreated:Put", "s3:ObjectCreated:*"));
        assert!(event_matches("s3:ObjectCreated:Post", "s3:ObjectCreated:*"));
        assert!(!event_matches(
            "s3:ObjectRemoved:Delete",
            "s3:ObjectCreated:*"
        ));
    }

    #[test]
    fn event_matches_global_wildcard() {
        assert!(event_matches("s3:ObjectCreated:Put", "s3:*"));
        assert!(event_matches("s3:ObjectRemoved:Delete", "s3:*"));
    }

    #[test]
    fn key_matches_filters_prefix() {
        let p = Some("logs/".to_string());
        assert!(key_matches_filters("logs/x.txt", &p, &None));
        assert!(!key_matches_filters("other/x.txt", &p, &None));
    }

    #[test]
    fn key_matches_filters_suffix() {
        let s = Some(".json".to_string());
        assert!(key_matches_filters("data.json", &None, &s));
        assert!(!key_matches_filters("data.txt", &None, &s));
    }

    #[test]
    fn key_matches_filters_both() {
        let p = Some("logs/".to_string());
        let s = Some(".gz".to_string());
        assert!(key_matches_filters("logs/2024.gz", &p, &s));
        assert!(!key_matches_filters("logs/2024.txt", &p, &s));
        assert!(!key_matches_filters("other/2024.gz", &p, &s));
    }

    #[test]
    fn key_matches_filters_no_constraints() {
        assert!(key_matches_filters("anything", &None, &None));
    }

    #[test]
    fn parse_s3_key_filters_prefix_and_suffix() {
        let xml = r#"<QueueConfiguration>
            <Filter>
                <S3Key>
                    <FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule>
                    <FilterRule><Name>suffix</Name><Value>.json</Value></FilterRule>
                </S3Key>
            </Filter>
        </QueueConfiguration>"#;
        let (p, s) = parse_s3_key_filters(xml);
        assert_eq!(p.as_deref(), Some("logs/"));
        assert_eq!(s.as_deref(), Some(".json"));
    }

    #[test]
    fn parse_s3_key_filters_missing_filter_block() {
        let xml = "<QueueConfiguration></QueueConfiguration>";
        let (p, s) = parse_s3_key_filters(xml);
        assert!(p.is_none());
        assert!(s.is_none());
    }

    #[test]
    fn parse_s3_key_filters_unknown_name_ignored() {
        let xml = r#"<QueueConfiguration>
            <Filter>
                <S3Key>
                    <FilterRule><Name>ContentType</Name><Value>ignored</Value></FilterRule>
                </S3Key>
            </Filter>
        </QueueConfiguration>"#;
        let (p, s) = parse_s3_key_filters(xml);
        assert!(p.is_none());
        assert!(s.is_none());
    }

    #[test]
    fn parse_replication_rules_extracts_status_prefix_dest() {
        let xml = r#"<ReplicationConfiguration>
            <Rule>
                <Status>Enabled</Status>
                <Prefix>docs/</Prefix>
                <Destination><Bucket>arn:aws:s3:::dest-bucket</Bucket></Destination>
            </Rule>
            <Rule>
                <Status>Disabled</Status>
                <Prefix>archive/</Prefix>
                <Destination><Bucket>arn:aws:s3:::archive-dest</Bucket></Destination>
            </Rule>
        </ReplicationConfiguration>"#;
        let rules = parse_replication_rules(xml);
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].status, "Enabled");
        assert_eq!(rules[0].prefix, "docs/");
        assert_eq!(rules[0].dest_bucket, "dest-bucket");
        assert_eq!(rules[1].status, "Disabled");
        assert_eq!(rules[1].dest_bucket, "archive-dest");
    }

    #[test]
    fn parse_replication_rules_empty_returns_empty() {
        assert!(parse_replication_rules("").is_empty());
    }

    #[test]
    fn normalize_notification_ids_inserts_id_when_missing() {
        let xml = "<NotificationConfiguration>\
            <QueueConfiguration><Queue>arn</Queue></QueueConfiguration>\
        </NotificationConfiguration>";
        let out = normalize_notification_ids(xml);
        // Original XML had no <Id>, output must now contain one
        assert!(out.contains("<Id>"));
        assert!(out.contains("<Queue>arn</Queue>"));
    }

    #[test]
    fn normalize_notification_ids_preserves_existing() {
        let xml = "<NotificationConfiguration>\
            <QueueConfiguration><Id>my-id</Id><Queue>arn</Queue></QueueConfiguration>\
        </NotificationConfiguration>";
        let out = normalize_notification_ids(xml);
        assert!(out.contains("<Id>my-id</Id>"));
    }

    #[test]
    fn extract_all_xml_values_multiple_matches() {
        let xml = "<list><Event>one</Event><Event>two</Event><Event>three</Event></list>";
        let vals = extract_all_xml_values(xml, "Event");
        assert_eq!(vals, vec!["one", "two", "three"]);
    }

    #[test]
    fn extract_all_xml_values_no_matches() {
        let xml = "<list></list>";
        let vals = extract_all_xml_values(xml, "Event");
        assert!(vals.is_empty());
    }

    #[test]
    fn parse_notification_config_queue_target() {
        let xml = r#"<NotificationConfiguration>
            <QueueConfiguration>
                <Id>q1</Id>
                <Queue>arn:aws:sqs:us-east-1:123:q</Queue>
                <Event>s3:ObjectCreated:*</Event>
                <Filter>
                    <S3Key><FilterRule><Name>prefix</Name><Value>in/</Value></FilterRule></S3Key>
                </Filter>
            </QueueConfiguration>
        </NotificationConfiguration>"#;
        let targets = parse_notification_config(xml);
        assert_eq!(targets.len(), 1);
        assert!(matches!(
            targets[0].target_type,
            NotificationTargetType::Sqs
        ));
        assert_eq!(targets[0].arn, "arn:aws:sqs:us-east-1:123:q");
        assert_eq!(targets[0].events, vec!["s3:ObjectCreated:*".to_string()]);
        assert_eq!(targets[0].prefix_filter.as_deref(), Some("in/"));
    }

    #[test]
    fn build_s3_event_notification_populates_envelope() {
        let event_str = build_s3_event_notification(
            "s3:ObjectCreated:Put",
            "my-bucket",
            "key.txt",
            42,
            "etag",
            "us-east-1",
        );
        let event: serde_json::Value = serde_json::from_str(&event_str).unwrap();
        let records = event["Records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["eventName"], "s3:ObjectCreated:Put");
        assert_eq!(records[0]["s3"]["bucket"]["name"], "my-bucket");
        assert_eq!(records[0]["s3"]["object"]["key"], "key.txt");
        assert_eq!(records[0]["s3"]["object"]["size"], 42);
        assert_eq!(records[0]["s3"]["object"]["eTag"], "etag");
        assert_eq!(records[0]["awsRegion"], "us-east-1");
    }

    #[test]
    fn normalize_replication_xml_inverted_tags_does_not_panic() {
        // A rule whose closing tags precede their opening tags would slice with
        // begin > end and panic (dropping the connection -- a reachable DoS via
        // PutBucketReplication). The normalizer must return without crashing
        // (bug-audit 2026-06-20, 2.2).
        let inverted_destination = "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
            </Destination>ZZZZZ<Destination></Rule></ReplicationConfiguration>";
        let _ = normalize_replication_xml(inverted_destination);

        let inverted_filter = "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
            </Filter>ZZZZZ<Filter></Rule></ReplicationConfiguration>";
        let _ = normalize_replication_xml(inverted_filter);

        let inverted_dmr = "<ReplicationConfiguration><Rule><Status>Enabled</Status>\
            </DeleteMarkerReplication>ZZ<DeleteMarkerReplication></Rule></ReplicationConfiguration>";
        let _ = normalize_replication_xml(inverted_dmr);
    }

    #[test]
    fn validate_lifecycle_xml_inverted_filter_tags_is_malformed_not_panic() {
        // `</Filter>` before `<Filter>` would slice the filter body with
        // begin > end and panic (reachable DoS via
        // PutBucketLifecycleConfiguration). It must be rejected as malformed
        // instead (bug-audit 2026-06-20, 2.1).
        let xml = "<LifecycleConfiguration><Rule><Status>Enabled</Status>\
            </Filter>XXXXXXXX<Filter></Rule></LifecycleConfiguration>";
        let result = crate::service::validate_lifecycle_xml(xml);
        assert!(result.is_err(), "inverted <Filter> tags must be malformed");
    }
}
