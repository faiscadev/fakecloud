use std::sync::Arc;

use chrono::Utc;

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::{CrossServiceEvent, DeliveryBus};

use crate::state::{S3NotificationEvent, SharedS3State};

use super::extract_xml_value;

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

/// Monotonically increasing counter backing the `sequencer` field AWS puts on
/// every S3 event record. Consumers use it to order events for the same key,
/// so it must strictly increase for the lifetime of the process even when two
/// events land in the same microsecond.
static SEQUENCER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Produce the next `sequencer` value: an uppercase hex string, like AWS.
pub(crate) fn next_sequencer() -> String {
    use std::sync::atomic::Ordering;
    let now = Utc::now().timestamp_micros().max(0) as u64;
    // Compare-and-swap loop (rather than `fetch_update`, deprecated in favor
    // of `try_update`, which is newer than the crate's MSRV).
    let mut prev = SEQUENCER.load(Ordering::SeqCst);
    loop {
        let next = now.max(prev.saturating_add(1));
        match SEQUENCER.compare_exchange_weak(prev, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return format!("{next:016X}"),
            Err(actual) => prev = actual,
        }
    }
}

/// URL-encode an object key the way S3 encodes it in event notifications:
/// UTF-8 percent-encoding with `+` for spaces, leaving `/` intact so the
/// key's path structure survives. Handlers written against real S3 run the
/// key through `unquote_plus`/`decodeURIComponent`, so emitting the raw key
/// silently corrupts any key containing a space or a `+`.
pub(crate) fn encode_event_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' | b'/' => {
                out.push(*b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Build an S3 event notification JSON payload.
///
/// `configuration_id` is the `Id` of the notification configuration that
/// matched (AWS reports which rule fired), `owner_account` the bucket owner,
/// and `sequencer` the per-event ordering token shared by every target of the
/// same event.
///
/// `requestParameters`/`responseElements` are deliberately omitted rather than
/// filled with invented values: the caller's source IP is not plumbed down to
/// this layer, and a fabricated IP is worse than an absent field for anyone
/// asserting on the record.
pub(crate) fn build_s3_event_notification(
    event: &ObjectEvent<'_>,
    configuration_id: Option<&str>,
    owner_account: &str,
    sequencer: &str,
) -> String {
    let event_time = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

    // ObjectRemoved records carry no size or eTag on real S3 -- there is no
    // object left to describe -- so emit only what AWS emits.
    let removed = event.event_name.starts_with("ObjectRemoved");
    let mut object = serde_json::json!({ "key": encode_event_key(event.key) });
    if !removed {
        object["size"] = serde_json::json!(event.size);
        object["eTag"] = serde_json::json!(event.etag);
    }
    // versionId is present only on versioning-enabled buckets.
    if let Some(vid) = event.version_id {
        object["versionId"] = serde_json::json!(vid);
    }
    object["sequencer"] = serde_json::json!(sequencer);

    let mut s3 = serde_json::json!({
        "s3SchemaVersion": "1.0",
        "bucket": {
            "name": event.bucket_name,
            "ownerIdentity": { "principalId": owner_account },
            "arn": Arn::s3_in(event.region, event.bucket_name).to_string()
        },
        "object": object
    });
    if let Some(id) = configuration_id {
        s3["configurationId"] = serde_json::json!(id);
    }

    serde_json::json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": event.region,
            "eventTime": event_time,
            "eventName": event.event_name,
            "userIdentity": { "principalId": format!("AWS:{}", event.requester_account) },
            "s3": s3
        }]
    })
    .to_string()
}

/// Map an S3 event name onto the EventBridge detail-type AWS publishes for it.
pub(crate) fn eventbridge_detail_type(event_name: &str) -> String {
    match event_name {
        n if n.starts_with("ObjectCreated") => "Object Created".to_string(),
        n if n.starts_with("ObjectRemoved") || n == "LifecycleExpiration:Delete" => {
            "Object Deleted".to_string()
        }
        "LifecycleExpiration:DeleteMarkerCreated" => "Object Deleted".to_string(),
        "ObjectRestore:Post" => "Object Restore Initiated".to_string(),
        "ObjectRestore:Completed" => "Object Restore Completed".to_string(),
        "ObjectRestore:Delete" => "Object Restore Expired".to_string(),
        "ObjectTagging:Put" => "Object Tags Added".to_string(),
        "ObjectTagging:Delete" => "Object Tags Deleted".to_string(),
        "ObjectAcl:Put" => "Object ACL Updated".to_string(),
        n if n.starts_with("LifecycleTransition") => "Object Storage Class Changed".to_string(),
        n if n.starts_with("IntelligentTiering") => "Object Access Tier Changed".to_string(),
        other => format!("Object {other}"),
    }
}

/// The API call AWS reports as the `reason` on an EventBridge S3 event.
fn eventbridge_reason(event_name: &str) -> Option<&'static str> {
    match event_name {
        "ObjectCreated:Put" => Some("PutObject"),
        "ObjectCreated:Post" => Some("POST Object"),
        "ObjectCreated:Copy" => Some("CopyObject"),
        "ObjectCreated:CompleteMultipartUpload" => Some("CompleteMultipartUpload"),
        "ObjectRemoved:Delete" | "ObjectRemoved:DeleteMarkerCreated" => Some("DeleteObject"),
        _ => None,
    }
}

/// Parsed notification target from the bucket notification config XML.
pub(crate) struct NotificationTarget {
    pub(crate) target_type: NotificationTargetType,
    pub(crate) arn: String,
    /// The configuration's `Id`, reported back as `configurationId` on every
    /// record this target receives.
    pub(crate) id: Option<String>,
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
                    id: extract_xml_value(block, "Id"),
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
                    id: extract_xml_value(block, "Id"),
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
                    id: extract_xml_value(block, "Id"),
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
                    id: extract_xml_value(block, "Id"),
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
    filter == "s3:*"
}

/// Everything a reader needs to describe a single S3 object-level event.
#[derive(Clone, Copy)]
pub(crate) struct ObjectEvent<'a> {
    pub event_name: &'a str,
    pub bucket_name: &'a str,
    pub key: &'a str,
    pub size: u64,
    pub etag: &'a str,
    pub region: &'a str,
    /// The account that made the request. AWS reports the *requester* on
    /// `userIdentity` / EventBridge `requester`, which is not necessarily the
    /// bucket owner once cross-account writes are in play.
    pub requester_account: &'a str,
    /// Version of the object the event describes. `Some` on a
    /// versioning-enabled bucket (including the id of a delete marker),
    /// `None` on an unversioned one -- matching when AWS includes
    /// `s3.object.versionId` on the record.
    pub version_id: Option<&'a str>,
}

/// Deliver S3 event notifications for a single bucket operation.
pub(crate) fn deliver_notifications(
    delivery: &Arc<DeliveryBus>,
    notification_config: &str,
    event: &ObjectEvent<'_>,
    s3_state: Option<&SharedS3State>,
) {
    deliver_notification_batch(
        delivery,
        notification_config,
        std::slice::from_ref(event),
        s3_state,
    );
}

/// Deliver several events produced by one operation on one bucket (the
/// `DeleteObjects` batch). The config is parsed and the bucket owner /
/// EventBridge flag resolved once for the whole batch rather than per object,
/// which matters for a 1000-key delete; each event still gets its own
/// sequencer and its own message, as on AWS. All events must be for the same
/// bucket -- the first event's bucket names the one whose state is read.
pub(crate) fn deliver_notification_batch(
    delivery: &Arc<DeliveryBus>,
    notification_config: &str,
    events: &[ObjectEvent<'_>],
    s3_state: Option<&SharedS3State>,
) {
    let Some(first) = events.first() else {
        return;
    };
    let bucket_name = first.bucket_name;
    debug_assert!(
        events.iter().all(|e| e.bucket_name == bucket_name),
        "a notification batch must describe a single bucket"
    );

    let targets = parse_notification_config(notification_config);

    // One pass over S3 state for the whole batch: the bucket owner fills the
    // bucket's `ownerIdentity` and is the account the EventBridge event is
    // delivered to, the bucket's region is the event's region, and the
    // EventBridge opt-in is per bucket.
    let (owner_account, bucket_region, eventbridge_enabled) = s3_state
        .and_then(|st| {
            let mas = st.read();
            let acct = mas.find_account(|s| s.buckets.contains_key(bucket_name))?;
            let bucket = mas.get(acct).and_then(|s| s.buckets.get(bucket_name))?;
            Some((
                acct.to_string(),
                bucket.region.clone(),
                bucket.eventbridge_enabled,
            ))
        })
        // Without S3 state (or once the bucket is gone) the owner cannot be
        // resolved; the requester and the request's region are the best
        // available attribution, and no EventBridge event is sent since the
        // per-bucket opt-in cannot be read.
        .unwrap_or_else(|| {
            (
                first.requester_account.to_string(),
                first.region.to_string(),
                false,
            )
        });
    let bucket_region = if bucket_region.is_empty() {
        first.region.to_string()
    } else {
        bucket_region
    };
    let bucket_arn = vec![Arn::s3_in(&bucket_region, bucket_name).to_string()];

    // Indices of the events that matched at least one target; only those are
    // recorded for introspection.
    let mut delivered: Vec<usize> = Vec::new();

    for (idx, event) in events.iter().enumerate() {
        // Every record reports the bucket's region, not the region of the
        // endpoint the request happened to reach.
        let event = &ObjectEvent {
            region: &bucket_region,
            ..*event
        };
        let event_name = event.event_name;
        let key = event.key;
        let s3_event_name = format!("s3:{event_name}");
        // Every target of one operation shares a sequencer, as on real S3.
        let sequencer = next_sequencer();

        if eventbridge_enabled {
            let mut object = serde_json::json!({ "key": key, "sequencer": sequencer });
            if !event_name.starts_with("ObjectRemoved") {
                object["size"] = serde_json::json!(event.size);
                object["etag"] = serde_json::json!(event.etag);
            }
            if let Some(vid) = event.version_id {
                object["version-id"] = serde_json::json!(vid);
            }
            let mut detail = serde_json::json!({
                "version": "0",
                "bucket": { "name": bucket_name },
                "object": object,
                "request-id": uuid::Uuid::new_v4().to_string(),
                "requester": event.requester_account,
            });
            if let Some(reason) = eventbridge_reason(event_name) {
                detail["reason"] = serde_json::json!(reason);
            }
            if event_name == "ObjectRemoved:Delete" {
                detail["deletion-type"] = serde_json::json!("Permanently deleted");
            } else if event_name == "ObjectRemoved:DeleteMarkerCreated" {
                detail["deletion-type"] = serde_json::json!("Delete marker created");
            }
            let detail = detail.to_string();
            // The owner's own default bus: never a cross-account put, so it
            // cannot be refused.
            if let Err(err) = delivery.put_event_to_eventbridge(&CrossServiceEvent {
                source: "aws.s3",
                detail_type: &eventbridge_detail_type(event_name),
                detail: &detail,
                event_bus: "default",
                account_id: &owner_account,
                region: &bucket_region,
                resources: &bucket_arn,
                principal_arn: None,
            }) {
                tracing::warn!(bucket = bucket_name, %err, "S3 EventBridge notification refused");
            }
        }

        let mut matched = false;

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
            matched = true;
            // configurationId names the rule that fired, so the message is
            // built per target rather than once per event.
            let message = build_s3_event_notification(
                event,
                target.id.as_deref(),
                &owner_account,
                &sequencer,
            );
            match target.target_type {
                NotificationTargetType::Sqs => {
                    // S3 delivers as its service principal, for the bucket:
                    // an encrypted queue's key policy must name
                    // `s3.amazonaws.com`.
                    delivery.send_to_sqs(
                        fakecloud_core::delivery::DeliverySender::Service {
                            principal: "s3.amazonaws.com",
                            source_arn: &bucket_arn[0],
                            source_account: &owner_account,
                        },
                        &target.arn,
                        &message,
                        &std::collections::HashMap::new(),
                    );
                }
                NotificationTargetType::Sns => {
                    delivery.publish_to_sns(&target.arn, &message, Some("Amazon S3 Notification"));
                }
                NotificationTargetType::Lambda => {
                    let function_arn = target.arn.clone();
                    let payload = message;
                    // Objects other services deliver can be written from a
                    // thread with no runtime; the invocation then has nowhere
                    // to run, so it is reported instead of panicking.
                    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                        tracing::warn!(
                            function_arn = %function_arn,
                            "S3->Lambda notification dropped: no async runtime on this thread"
                        );
                        continue;
                    };
                    // S3 invokes the function asynchronously; a function
                    // error is Lambda's to retry and route, not S3's.
                    let _entered = runtime.enter();
                    match delivery.invoke_lambda_async(&function_arn, &payload) {
                        Some(Ok(())) => {
                            tracing::info!(
                                function_arn = %function_arn,
                                "S3->Lambda notification queued"
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
                }
            }
        }

        if matched {
            delivered.push(idx);
        }
    }

    // Record notification events for introspection, only for the events that
    // actually matched a target. One write lock for the batch.
    if !delivered.is_empty() {
        if let Some(state) = s3_state {
            let mut mas = state.write();
            let owner_acct = mas
                .find_account(|s| s.buckets.contains_key(bucket_name))
                .map(|a| a.to_string());
            if let Some(acct) = owner_acct {
                if let Some(acct_state) = mas.get_mut(&acct) {
                    for event in delivered.iter().map(|&i| &events[i]) {
                        acct_state.notification_events.push(S3NotificationEvent {
                            bucket: bucket_name.to_string(),
                            key: event.key.to_string(),
                            event_type: format!("s3:{}", event.event_name),
                            timestamp: Utc::now(),
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::replication::{normalize_replication_xml, parse_replication_rules};

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
    fn parse_replication_rules_reads_account_and_owner_translation() {
        let xml = r#"<ReplicationConfiguration><Rule><Status>Enabled</Status>
            <Destination><Bucket>arn:aws:s3:::dest</Bucket><Account>222222222222</Account>
            <AccessControlTranslation><Owner>Destination</Owner></AccessControlTranslation>
            </Destination></Rule>
            <Rule><Status>Enabled</Status>
            <Destination><Bucket>arn:aws:s3:::plain</Bucket></Destination></Rule>
            </ReplicationConfiguration>"#;
        let rules = parse_replication_rules(xml);
        assert_eq!(rules[0].dest_account.as_deref(), Some("222222222222"));
        assert!(rules[0].owner_override);
        assert_eq!(rules[1].dest_account, None);
        assert!(!rules[1].owner_override);
    }

    /// (bus, account, region, resources) of each recorded event.
    type EbCall = (String, String, String, Vec<String>);

    #[derive(Default)]
    struct EbRecorder(std::sync::Mutex<Vec<EbCall>>);

    impl fakecloud_core::delivery::EventBridgeDelivery for EbRecorder {
        fn put_event(
            &self,
            e: &CrossServiceEvent<'_>,
        ) -> Result<String, fakecloud_core::delivery::EventBridgeDeliveryError> {
            self.0.lock().unwrap().push((
                e.event_bus.to_string(),
                e.account_id.to_string(),
                e.region.to_string(),
                e.resources.to_vec(),
            ));
            Ok("id".to_string())
        }
    }

    /// S3's EventBridge event goes to the bucket owner's default bus and
    /// carries the bucket's region, whatever account made the request and
    /// whatever the server's startup account/region are.
    #[test]
    fn eventbridge_event_is_scoped_to_bucket_owner_and_region() {
        let state: SharedS3State = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
        ));
        {
            let mut mas = state.write();
            let mut bucket = crate::state::S3Bucket::new("my-bucket", "eu-west-2", "111111111111");
            bucket.eventbridge_enabled = true;
            mas.get_or_create("111111111111")
                .buckets
                .insert("my-bucket".to_string(), bucket);
        }
        let recorder = Arc::new(EbRecorder::default());
        let bus = Arc::new(DeliveryBus::new().with_eventbridge(recorder.clone()));
        // The request reached the us-east-1 endpoint from another account.
        deliver_notifications(
            &bus,
            "<NotificationConfiguration><EventBridgeConfiguration/></NotificationConfiguration>",
            &ev("ObjectCreated:Put", "k", None),
            Some(&state),
        );
        let calls = recorder.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "default");
        assert_eq!(calls[0].1, "111111111111");
        assert_eq!(calls[0].2, "eu-west-2");
        assert_eq!(calls[0].3, vec!["arn:aws:s3:::my-bucket".to_string()]);
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

    fn ev<'a>(event_name: &'a str, key: &'a str, version_id: Option<&'a str>) -> ObjectEvent<'a> {
        ObjectEvent {
            event_name,
            bucket_name: "my-bucket",
            requester_account: "999999999999",
            key,
            size: 42,
            etag: "etag",
            region: "us-east-1",
            version_id,
        }
    }

    #[test]
    fn build_s3_event_notification_populates_envelope() {
        let event_str =
            build_s3_event_notification(&ev("ObjectCreated:Put", "key.txt", None), None, "1", "A");
        let event: serde_json::Value = serde_json::from_str(&event_str).unwrap();
        let records = event["Records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["eventName"], "ObjectCreated:Put");
        assert_eq!(records[0]["s3"]["bucket"]["name"], "my-bucket");
        assert_eq!(records[0]["s3"]["object"]["key"], "key.txt");
        assert_eq!(records[0]["s3"]["object"]["size"], 42);
        assert_eq!(records[0]["s3"]["object"]["eTag"], "etag");
        assert_eq!(records[0]["awsRegion"], "us-east-1");
        assert_eq!(records[0]["s3"]["s3SchemaVersion"], "1.0");
        assert_eq!(records[0]["s3"]["object"]["sequencer"], "A");
        assert_eq!(
            records[0]["s3"]["bucket"]["ownerIdentity"]["principalId"],
            "1"
        );
        // ownerIdentity is the bucket owner; userIdentity is the requester.
        assert_eq!(
            records[0]["userIdentity"]["principalId"],
            "AWS:999999999999"
        );
        // No versioning -> no versionId, like AWS.
        assert!(records[0]["s3"]["object"].get("versionId").is_none());
        // No configuration Id parsed -> field omitted rather than null.
        assert!(records[0]["s3"].get("configurationId").is_none());
    }

    #[test]
    fn build_s3_event_notification_includes_version_id() {
        let event_str = build_s3_event_notification(
            &ev("ObjectCreated:Put", "key.txt", Some("v-1")),
            Some("cfg-id"),
            "123456789012",
            "0055AED6DCD90281E5",
        );
        let event: serde_json::Value = serde_json::from_str(&event_str).unwrap();
        let record = &event["Records"][0];
        assert_eq!(record["s3"]["object"]["versionId"], "v-1");
        assert_eq!(record["s3"]["configurationId"], "cfg-id");
    }

    #[test]
    fn build_s3_event_notification_removed_omits_size_and_etag() {
        // Real S3 sends neither size nor eTag on ObjectRemoved records.
        let event_str = build_s3_event_notification(
            &ev("ObjectRemoved:DeleteMarkerCreated", "key.txt", Some("dm-1")),
            None,
            "123456789012",
            "SEQ",
        );
        let event: serde_json::Value = serde_json::from_str(&event_str).unwrap();
        let object = &event["Records"][0]["s3"]["object"];
        assert!(object.get("size").is_none());
        assert!(object.get("eTag").is_none());
        assert_eq!(object["versionId"], "dm-1");
    }

    #[test]
    fn event_key_is_url_encoded_like_aws() {
        assert_eq!(encode_event_key("my file.txt"), "my+file.txt");
        assert_eq!(encode_event_key("a/b/c.txt"), "a/b/c.txt");
        assert_eq!(encode_event_key("a+b.txt"), "a%2Bb.txt");
        assert_eq!(encode_event_key("caf\u{e9}.txt"), "caf%C3%A9.txt");
        assert_eq!(encode_event_key("plain-key_1.txt"), "plain-key_1.txt");
    }

    #[test]
    fn sequencer_strictly_increases() {
        let a = next_sequencer();
        let b = next_sequencer();
        let c = next_sequencer();
        assert!(a < b && b < c, "{a} {b} {c}");
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn eventbridge_detail_types_match_aws() {
        assert_eq!(
            eventbridge_detail_type("ObjectCreated:Put"),
            "Object Created"
        );
        assert_eq!(
            eventbridge_detail_type("ObjectCreated:CompleteMultipartUpload"),
            "Object Created"
        );
        assert_eq!(
            eventbridge_detail_type("ObjectRemoved:Delete"),
            "Object Deleted"
        );
        assert_eq!(
            eventbridge_detail_type("ObjectRestore:Post"),
            "Object Restore Initiated"
        );
        assert_eq!(
            eventbridge_detail_type("ObjectTagging:Put"),
            "Object Tags Added"
        );
        assert_eq!(
            eventbridge_detail_type("ObjectAcl:Put"),
            "Object ACL Updated"
        );
    }

    #[test]
    fn parse_notification_config_captures_configuration_id() {
        let xml = r#"<NotificationConfiguration>
            <QueueConfiguration>
                <Id>my-rule</Id>
                <Queue>arn:aws:sqs:us-east-1:123:q</Queue>
                <Event>s3:ObjectCreated:*</Event>
            </QueueConfiguration>
        </NotificationConfiguration>"#;
        let targets = parse_notification_config(xml);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].id.as_deref(), Some("my-rule"));
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
