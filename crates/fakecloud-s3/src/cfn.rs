//! Translate CloudFormation `AWS::S3::Bucket` template properties into the
//! internal S3 bucket state.
//!
//! The CloudFormation provisioner used to build a bucket from its `BucketName`
//! alone, silently dropping every other modeled property (versioning,
//! encryption, public-access-block, notifications, tags, website, CORS,
//! lifecycle, logging). A `CREATE_COMPLETE` bucket then read back with none of
//! its protections, and S3 -> Lambda/SQS/SNS notifications never fired.
//!
//! Each helper here maps one CFN property object into the exact XML (or state
//! field) the matching `Put*` REST handler stores, then persists it through the
//! same [`S3Store`] path those handlers use — so a CFN-provisioned bucket
//! round-trips through `GetBucketVersioning` / `GetBucketEncryption` /
//! `GetPublicAccessBlock` / `GetBucketNotification` / `GetBucketTagging` (etc.)
//! and survives a restart, matching the `PutBucket*` API exactly.
//!
//! CFN property shapes differ from the REST API XML shapes (e.g.
//! `VersioningConfiguration.Status`,
//! `BucketEncryption.ServerSideEncryptionConfiguration[]`,
//! `NotificationConfiguration.LambdaConfigurations[].Function`), so the mapping
//! is explicit rather than a passthrough.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;

use fakecloud_aws::xml::xml_escape;
use fakecloud_persistence::{BucketSubresource, S3Store, StoreError, TagsSnapshot};

use crate::persistence::bucket_meta_snapshot;
use crate::service::notifications::normalize_notification_ids;
use crate::state::S3Bucket;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Apply the config properties of an `AWS::S3::Bucket` CloudFormation resource
/// to `bucket`, persisting each touched subresource through `store` exactly as
/// the corresponding `PutBucket*` handler does. Used by both the create and
/// update provisioner paths so a stack that turns on versioning/encryption in a
/// follow-up update is not a silent no-op.
///
/// Only properties actually present in `props` are applied; absent properties
/// leave the existing bucket state untouched (matching CFN update semantics for
/// the properties we model).
pub fn apply_cfn_bucket_properties(
    bucket: &mut S3Bucket,
    props: &Value,
    store: &Arc<dyn S3Store>,
) -> Result<(), String> {
    let Some(obj) = props.as_object() else {
        return Ok(());
    };
    // `versioning` and `eventbridge_enabled` live in the bucket meta snapshot,
    // so a single `put_bucket_meta` at the end covers both — mirroring how the
    // versioning/notification handlers persist them.
    let mut meta_dirty = false;

    if let Some(v) = obj.get("VersioningConfiguration") {
        if let Some(status) = v.get("Status").and_then(Value::as_str) {
            if status == "Enabled" || status == "Suspended" {
                bucket.versioning = Some(status.to_string());
                meta_dirty = true;
            }
        }
    }

    if let Some(enc) = obj.get("BucketEncryption") {
        if let Some(xml) = build_encryption_xml(enc) {
            bucket.encryption_config = Some(xml.clone());
            persist_sub(store, &bucket.name, BucketSubresource::Encryption, &xml)?;
        }
    }

    if let Some(pab) = obj.get("PublicAccessBlockConfiguration") {
        if let Some(xml) = build_public_access_block_xml(pab) {
            bucket.public_access_block = Some(xml.clone());
            persist_sub(
                store,
                &bucket.name,
                BucketSubresource::PublicAccessBlock,
                &xml,
            )?;
        }
    }

    if let Some(nc) = obj.get("NotificationConfiguration") {
        let (xml, eventbridge) = build_notification_xml(nc);
        bucket.notification_config = Some(xml.clone());
        bucket.eventbridge_enabled = eventbridge;
        meta_dirty = true;
        persist_sub(store, &bucket.name, BucketSubresource::Notification, &xml)?;
    }

    if let Some(tags) = obj.get("Tags") {
        if let Some(map) = build_tags(tags) {
            bucket.tags = map;
            let payload = toml::to_string(&TagsSnapshot {
                tags: bucket.tags.clone(),
            })
            .unwrap_or_default();
            persist_sub(store, &bucket.name, BucketSubresource::Tags, &payload)?;
        }
    }

    if let Some(w) = obj.get("WebsiteConfiguration") {
        if let Some(xml) = build_website_xml(w) {
            bucket.website_config = Some(xml.clone());
            persist_sub(store, &bucket.name, BucketSubresource::Website, &xml)?;
        }
    }

    if let Some(c) = obj.get("CorsConfiguration") {
        if let Some(xml) = build_cors_xml(c) {
            bucket.cors_config = Some(xml.clone());
            persist_sub(store, &bucket.name, BucketSubresource::Cors, &xml)?;
        }
    }

    if let Some(l) = obj.get("LifecycleConfiguration") {
        if let Some(xml) = build_lifecycle_xml(l) {
            bucket.lifecycle_config = Some(xml.clone());
            persist_sub(store, &bucket.name, BucketSubresource::Lifecycle, &xml)?;
        }
    }

    if let Some(lg) = obj.get("LoggingConfiguration") {
        if let Some(xml) = build_logging_xml(lg) {
            bucket.logging_config = Some(xml.clone());
            persist_sub(store, &bucket.name, BucketSubresource::Logging, &xml)?;
        }
    }

    if meta_dirty {
        let meta = bucket_meta_snapshot(bucket);
        store
            .put_bucket_meta(&bucket.name, &meta)
            .map_err(|e| persist_err("meta", &bucket.name, e))?;
    }
    Ok(())
}

fn persist_sub(
    store: &Arc<dyn S3Store>,
    bucket: &str,
    kind: BucketSubresource,
    body: &str,
) -> Result<(), String> {
    store
        .put_bucket_subresource(bucket, kind, body)
        .map_err(|e| persist_err(&format!("{kind:?}"), bucket, e))
}

fn persist_err(what: &str, bucket: &str, e: StoreError) -> String {
    format!("failed to persist {what} for bucket {bucket}: {e}")
}

/// Parse a CFN JSON value into an optional bool, accepting both native JSON
/// booleans and the stringified forms (`"true"`/`"false"`) CFN templates and
/// resolved intrinsics can emit.
fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.trim() {
            "true" | "True" | "TRUE" => Some(true),
            "false" | "False" | "FALSE" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Render a CFN scalar (string / number / bool) as the string form its REST XML
/// element carries.
fn as_scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn wrap(root: &str, body: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><{root} xmlns=\"{XMLNS}\">{body}</{root}>")
}

// ---- BucketEncryption ----

fn build_encryption_xml(enc: &Value) -> Option<String> {
    let rules = enc.get("ServerSideEncryptionConfiguration")?.as_array()?;
    let mut body = String::new();
    for rule in rules {
        let by_default = rule.get("ServerSideEncryptionByDefault");
        let Some(algo) = by_default
            .and_then(|d| d.get("SSEAlgorithm"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let kms_key = by_default
            .and_then(|d| d.get("KMSMasterKeyID"))
            .and_then(Value::as_str);
        let bucket_key = rule
            .get("BucketKeyEnabled")
            .and_then(as_bool)
            .unwrap_or(false);
        body.push_str("<Rule><ApplyServerSideEncryptionByDefault>");
        body.push_str(&format!(
            "<SSEAlgorithm>{}</SSEAlgorithm>",
            xml_escape(algo)
        ));
        if let Some(key) = kms_key {
            body.push_str(&format!(
                "<KMSMasterKeyID>{}</KMSMasterKeyID>",
                xml_escape(key)
            ));
        }
        body.push_str("</ApplyServerSideEncryptionByDefault>");
        body.push_str(&format!(
            "<BucketKeyEnabled>{bucket_key}</BucketKeyEnabled>"
        ));
        body.push_str("</Rule>");
    }
    if body.is_empty() {
        return None;
    }
    Some(wrap("ServerSideEncryptionConfiguration", &body))
}

// ---- PublicAccessBlockConfiguration ----

fn build_public_access_block_xml(pab: &Value) -> Option<String> {
    let obj = pab.as_object()?;
    let mut body = String::new();
    // The CFN property keys match the REST element names 1:1. `GetPublicAccessBlock`
    // fills any missing field with `false`, so emitting only the provided ones
    // round-trips faithfully.
    for key in [
        "BlockPublicAcls",
        "IgnorePublicAcls",
        "BlockPublicPolicy",
        "RestrictPublicBuckets",
    ] {
        if let Some(b) = obj.get(key).and_then(as_bool) {
            body.push_str(&format!("<{key}>{b}</{key}>"));
        }
    }
    if body.is_empty() {
        return None;
    }
    Some(wrap("PublicAccessBlockConfiguration", &body))
}

// ---- NotificationConfiguration ----

/// Build the notification-config XML and return whether EventBridge delivery is
/// enabled. Matches the tag shapes `parse_notification_config` reads on the
/// firing path (`<QueueConfiguration>/<Queue>`, `<TopicConfiguration>/<Topic>`,
/// `<LambdaFunctionConfiguration>/<Function>`), so notifications actually fire.
fn build_notification_xml(nc: &Value) -> (String, bool) {
    let mut body = String::new();
    build_notification_entries(
        nc,
        "QueueConfigurations",
        "QueueConfiguration",
        "Queue",
        &mut body,
    );
    build_notification_entries(
        nc,
        "TopicConfigurations",
        "TopicConfiguration",
        "Topic",
        &mut body,
    );
    build_notification_entries(
        nc,
        "LambdaConfigurations",
        "LambdaFunctionConfiguration",
        "Function",
        &mut body,
    );

    // EventBridge: presence of the key enables it unless EventBridgeEnabled is
    // explicitly false.
    let eventbridge = nc
        .get("EventBridgeConfiguration")
        .map(|eb| {
            eb.get("EventBridgeEnabled")
                .and_then(as_bool)
                .unwrap_or(true)
        })
        .unwrap_or(false);
    if eventbridge {
        body.push_str("<EventBridgeConfiguration></EventBridgeConfiguration>");
    }
    let xml = wrap("NotificationConfiguration", &body);
    // Auto-assign an <Id> to each configuration lacking one, as
    // PutBucketNotification does.
    (normalize_notification_ids(&xml), eventbridge)
}

fn build_notification_entries(
    nc: &Value,
    plural_key: &str,
    element: &str,
    target_tag: &str,
    out: &mut String,
) {
    let Some(arr) = nc.get(plural_key).and_then(Value::as_array) else {
        return;
    };
    for entry in arr {
        let Some(arn) = entry.get(target_tag).and_then(Value::as_str) else {
            continue;
        };
        out.push_str(&format!("<{element}>"));
        if let Some(id) = entry.get("Id").and_then(Value::as_str) {
            out.push_str(&format!("<Id>{}</Id>", xml_escape(id)));
        }
        out.push_str(&format!("<{target_tag}>{}</{target_tag}>", xml_escape(arn)));
        // CFN carries a single Event string per configuration.
        if let Some(ev) = entry.get("Event").and_then(Value::as_str) {
            out.push_str(&format!("<Event>{}</Event>", xml_escape(ev)));
        }
        out.push_str(&build_notification_filter(entry));
        out.push_str(&format!("</{element}>"));
    }
}

fn build_notification_filter(entry: &Value) -> String {
    let Some(rules) = entry
        .pointer("/Filter/S3Key/Rules")
        .and_then(Value::as_array)
    else {
        return String::new();
    };
    if rules.is_empty() {
        return String::new();
    }
    let mut out = String::from("<Filter><S3Key>");
    for rule in rules {
        let name = rule.get("Name").and_then(Value::as_str).unwrap_or("");
        let value = rule.get("Value").and_then(Value::as_str).unwrap_or("");
        out.push_str(&format!(
            "<FilterRule><Name>{}</Name><Value>{}</Value></FilterRule>",
            xml_escape(name),
            xml_escape(value)
        ));
    }
    out.push_str("</S3Key></Filter>");
    out
}

// ---- Tags ----

fn build_tags(tags: &Value) -> Option<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    match tags {
        Value::Array(arr) => {
            for t in arr {
                let key = t.get("Key").and_then(Value::as_str);
                let value = t.get("Value").and_then(Value::as_str);
                if let (Some(k), Some(v)) = (key, value) {
                    map.insert(k.to_string(), v.to_string());
                }
            }
        }
        Value::Object(obj) => {
            for (k, v) in obj {
                if let Some(v) = v.as_str() {
                    map.insert(k.clone(), v.to_string());
                }
            }
        }
        _ => return None,
    }
    if map.is_empty() {
        return None;
    }
    Some(map)
}

// ---- WebsiteConfiguration ----

fn build_website_xml(w: &Value) -> Option<String> {
    let obj = w.as_object()?;
    let mut body = String::new();
    if let Some(redirect) = obj.get("RedirectAllRequestsTo") {
        // When RedirectAllRequestsTo is set it is the only valid config.
        if let Some(host) = redirect.get("HostName").and_then(Value::as_str) {
            body.push_str("<RedirectAllRequestsTo>");
            body.push_str(&format!("<HostName>{}</HostName>", xml_escape(host)));
            if let Some(proto) = redirect.get("Protocol").and_then(Value::as_str) {
                body.push_str(&format!("<Protocol>{}</Protocol>", xml_escape(proto)));
            }
            body.push_str("</RedirectAllRequestsTo>");
        }
    } else {
        // CFN models IndexDocument/ErrorDocument as plain strings; REST nests
        // them under <Suffix>/<Key>.
        if let Some(idx) = obj.get("IndexDocument").and_then(Value::as_str) {
            body.push_str(&format!(
                "<IndexDocument><Suffix>{}</Suffix></IndexDocument>",
                xml_escape(idx)
            ));
        }
        if let Some(err) = obj.get("ErrorDocument").and_then(Value::as_str) {
            body.push_str(&format!(
                "<ErrorDocument><Key>{}</Key></ErrorDocument>",
                xml_escape(err)
            ));
        }
        if let Some(rules) = obj.get("RoutingRules").and_then(Value::as_array) {
            body.push_str(&build_routing_rules(rules));
        }
    }
    if body.is_empty() {
        return None;
    }
    Some(wrap("WebsiteConfiguration", &body))
}

fn build_routing_rules(rules: &[Value]) -> String {
    if rules.is_empty() {
        return String::new();
    }
    let mut out = String::from("<RoutingRules>");
    for rule in rules {
        out.push_str("<RoutingRule>");
        if let Some(cond) = rule.get("RoutingRuleCondition") {
            let mut c = String::new();
            if let Some(v) = cond.get("KeyPrefixEquals").and_then(Value::as_str) {
                c.push_str(&format!(
                    "<KeyPrefixEquals>{}</KeyPrefixEquals>",
                    xml_escape(v)
                ));
            }
            if let Some(v) = cond.get("HttpErrorCodeReturnedEquals").and_then(as_scalar) {
                c.push_str(&format!(
                    "<HttpErrorCodeReturnedEquals>{}</HttpErrorCodeReturnedEquals>",
                    xml_escape(&v)
                ));
            }
            if !c.is_empty() {
                out.push_str(&format!("<Condition>{c}</Condition>"));
            }
        }
        if let Some(redir) = rule.get("RedirectRule") {
            let mut r = String::new();
            for field in [
                "HostName",
                "HttpRedirectCode",
                "Protocol",
                "ReplaceKeyPrefixWith",
                "ReplaceKeyWith",
            ] {
                if let Some(v) = redir.get(field).and_then(as_scalar) {
                    r.push_str(&format!("<{field}>{}</{field}>", xml_escape(&v)));
                }
            }
            out.push_str(&format!("<Redirect>{r}</Redirect>"));
        }
        out.push_str("</RoutingRule>");
    }
    out.push_str("</RoutingRules>");
    out
}

// ---- CorsConfiguration ----

fn build_cors_xml(c: &Value) -> Option<String> {
    let rules = c.get("CorsRules")?.as_array()?;
    let mut body = String::new();
    for rule in rules {
        body.push_str("<CORSRule>");
        if let Some(id) = rule.get("Id").and_then(Value::as_str) {
            body.push_str(&format!("<ID>{}</ID>", xml_escape(id)));
        }
        push_string_list(&mut body, rule.get("AllowedHeaders"), "AllowedHeader");
        push_string_list(&mut body, rule.get("AllowedMethods"), "AllowedMethod");
        push_string_list(&mut body, rule.get("AllowedOrigins"), "AllowedOrigin");
        push_string_list(&mut body, rule.get("ExposedHeaders"), "ExposeHeader");
        if let Some(max_age) = rule.get("MaxAge").and_then(as_scalar) {
            body.push_str(&format!(
                "<MaxAgeSeconds>{}</MaxAgeSeconds>",
                xml_escape(&max_age)
            ));
        }
        body.push_str("</CORSRule>");
    }
    if body.is_empty() {
        return None;
    }
    Some(wrap("CORSConfiguration", &body))
}

fn push_string_list(out: &mut String, val: Option<&Value>, tag: &str) {
    if let Some(arr) = val.and_then(Value::as_array) {
        for item in arr {
            if let Some(s) = item.as_str() {
                out.push_str(&format!("<{tag}>{}</{tag}>", xml_escape(s)));
            }
        }
    }
}

// ---- LifecycleConfiguration ----

fn build_lifecycle_xml(l: &Value) -> Option<String> {
    let rules = l.get("Rules")?.as_array()?;
    let mut body = String::new();
    for rule in rules {
        body.push_str("<Rule>");
        if let Some(id) = rule.get("Id").and_then(Value::as_str) {
            body.push_str(&format!("<ID>{}</ID>", xml_escape(id)));
        }
        let status = rule
            .get("Status")
            .and_then(Value::as_str)
            .unwrap_or("Enabled");
        body.push_str(&format!("<Status>{}</Status>", xml_escape(status)));
        // Always emit a <Filter> (never a rule-level <Prefix>) so the config
        // passes `validate_lifecycle_xml`, which rejects rules carrying both.
        body.push_str(&build_lifecycle_filter(rule));
        body.push_str(&build_lifecycle_expiration(rule));
        for t in transitions(rule, "Transitions", "Transition") {
            let mut tb = String::new();
            if let Some(sc) = t.get("StorageClass").and_then(Value::as_str) {
                tb.push_str(&format!("<StorageClass>{}</StorageClass>", xml_escape(sc)));
            }
            if let Some(d) = t.get("TransitionInDays").and_then(as_scalar) {
                tb.push_str(&format!("<Days>{}</Days>", xml_escape(&d)));
            }
            if let Some(date) = t.get("TransitionDate").and_then(Value::as_str) {
                tb.push_str(&format!("<Date>{}</Date>", xml_escape(date)));
            }
            if !tb.is_empty() {
                body.push_str(&format!("<Transition>{tb}</Transition>"));
            }
        }
        body.push_str(&build_noncurrent_expiration(rule));
        for t in transitions(
            rule,
            "NoncurrentVersionTransitions",
            "NoncurrentVersionTransition",
        ) {
            // `validate_lifecycle_xml` requires both NoncurrentDays and StorageClass.
            let sc = t.get("StorageClass").and_then(Value::as_str);
            let days = t.get("TransitionInDays").and_then(as_scalar);
            if let (Some(sc), Some(days)) = (sc, days) {
                let mut tb = format!(
                    "<NoncurrentDays>{}</NoncurrentDays><StorageClass>{}</StorageClass>",
                    xml_escape(&days),
                    xml_escape(sc)
                );
                if let Some(n) = t.get("NewerNoncurrentVersions").and_then(as_scalar) {
                    tb.push_str(&format!(
                        "<NewerNoncurrentVersions>{}</NewerNoncurrentVersions>",
                        xml_escape(&n)
                    ));
                }
                body.push_str(&format!(
                    "<NoncurrentVersionTransition>{tb}</NoncurrentVersionTransition>"
                ));
            }
        }
        if let Some(abort) = rule.get("AbortIncompleteMultipartUpload") {
            if let Some(d) = abort.get("DaysAfterInitiation").and_then(as_scalar) {
                body.push_str(&format!(
                    "<AbortIncompleteMultipartUpload><DaysAfterInitiation>{}</DaysAfterInitiation></AbortIncompleteMultipartUpload>",
                    xml_escape(&d)
                ));
            }
        }
        body.push_str("</Rule>");
    }
    if body.is_empty() {
        return None;
    }
    Some(wrap("LifecycleConfiguration", &body))
}

fn build_lifecycle_filter(rule: &Value) -> String {
    let mut conds: Vec<String> = Vec::new();
    if let Some(p) = rule.get("Prefix").and_then(Value::as_str) {
        conds.push(format!("<Prefix>{}</Prefix>", xml_escape(p)));
    }
    if let Some(gt) = rule.get("ObjectSizeGreaterThan").and_then(as_scalar) {
        conds.push(format!(
            "<ObjectSizeGreaterThan>{}</ObjectSizeGreaterThan>",
            xml_escape(&gt)
        ));
    }
    if let Some(lt) = rule.get("ObjectSizeLessThan").and_then(as_scalar) {
        conds.push(format!(
            "<ObjectSizeLessThan>{}</ObjectSizeLessThan>",
            xml_escape(&lt)
        ));
    }
    if let Some(tags) = rule.get("TagFilters").and_then(Value::as_array) {
        for t in tags {
            let k = t.get("Key").and_then(Value::as_str).unwrap_or("");
            let v = t.get("Value").and_then(Value::as_str).unwrap_or("");
            conds.push(format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v)
            ));
        }
    }
    match conds.len() {
        0 => "<Filter></Filter>".to_string(),
        1 => format!("<Filter>{}</Filter>", conds[0]),
        // Multiple predicates must be wrapped in <And> (schema requirement the
        // validator enforces for Prefix+Tag combinations).
        _ => format!("<Filter><And>{}</And></Filter>", conds.concat()),
    }
}

fn build_lifecycle_expiration(rule: &Value) -> String {
    let mut exp = String::new();
    if let Some(days) = rule.get("ExpirationInDays").and_then(as_scalar) {
        exp.push_str(&format!("<Days>{}</Days>", xml_escape(&days)));
    }
    if let Some(date) = rule.get("ExpirationDate").and_then(Value::as_str) {
        exp.push_str(&format!("<Date>{}</Date>", xml_escape(date)));
    }
    // ExpiredObjectDeleteMarker is mutually exclusive with Days/Date.
    if exp.is_empty() {
        if let Some(b) = rule.get("ExpiredObjectDeleteMarker").and_then(as_bool) {
            exp.push_str(&format!(
                "<ExpiredObjectDeleteMarker>{b}</ExpiredObjectDeleteMarker>"
            ));
        }
    }
    if exp.is_empty() {
        String::new()
    } else {
        format!("<Expiration>{exp}</Expiration>")
    }
}

fn build_noncurrent_expiration(rule: &Value) -> String {
    if let Some(nve) = rule.get("NoncurrentVersionExpiration") {
        let mut nb = String::new();
        if let Some(d) = nve.get("NoncurrentDays").and_then(as_scalar) {
            nb.push_str(&format!(
                "<NoncurrentDays>{}</NoncurrentDays>",
                xml_escape(&d)
            ));
        }
        if let Some(n) = nve.get("NewerNoncurrentVersions").and_then(as_scalar) {
            nb.push_str(&format!(
                "<NewerNoncurrentVersions>{}</NewerNoncurrentVersions>",
                xml_escape(&n)
            ));
        }
        if !nb.is_empty() {
            return format!("<NoncurrentVersionExpiration>{nb}</NoncurrentVersionExpiration>");
        }
    } else if let Some(d) = rule
        .get("NoncurrentVersionExpirationInDays")
        .and_then(as_scalar)
    {
        return format!(
            "<NoncurrentVersionExpiration><NoncurrentDays>{}</NoncurrentDays></NoncurrentVersionExpiration>",
            xml_escape(&d)
        );
    }
    String::new()
}

/// Collect both the plural list form and the deprecated singular form of a
/// transition-style property into one list of objects.
fn transitions<'a>(rule: &'a Value, plural: &str, single: &str) -> Vec<&'a Value> {
    let mut out = Vec::new();
    if let Some(arr) = rule.get(plural).and_then(Value::as_array) {
        out.extend(arr.iter());
    }
    if let Some(one) = rule.get(single) {
        if one.is_object() {
            out.push(one);
        }
    }
    out
}

// ---- LoggingConfiguration ----

fn build_logging_xml(lg: &Value) -> Option<String> {
    let obj = lg.as_object()?;
    let dest = obj.get("DestinationBucketName").and_then(Value::as_str)?;
    let prefix = obj
        .get("LogFilePrefix")
        .and_then(Value::as_str)
        .unwrap_or("");
    let body = format!(
        "<LoggingEnabled><TargetBucket>{}</TargetBucket><TargetPrefix>{}</TargetPrefix></LoggingEnabled>",
        xml_escape(dest),
        xml_escape(prefix)
    );
    Some(wrap("BucketLoggingStatus", &body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store() -> Arc<dyn S3Store> {
        Arc::new(fakecloud_persistence::MemoryS3Store)
    }

    fn bucket() -> S3Bucket {
        S3Bucket::new("b", "us-east-1", "123456789012")
    }

    #[test]
    fn versioning_status_enabled() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({ "VersioningConfiguration": { "Status": "Enabled" } }),
            &store(),
        )
        .unwrap();
        assert_eq!(b.versioning.as_deref(), Some("Enabled"));
    }

    #[test]
    fn versioning_invalid_status_ignored() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({ "VersioningConfiguration": { "Status": "Bogus" } }),
            &store(),
        )
        .unwrap();
        assert!(b.versioning.is_none());
    }

    #[test]
    fn encryption_kms_rule() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{
                        "BucketKeyEnabled": true,
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": "arn:aws:kms:us-east-1:123456789012:key/abc"
                        }
                    }]
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.encryption_config.unwrap();
        assert!(xml.contains("<SSEAlgorithm>aws:kms</SSEAlgorithm>"));
        assert!(xml.contains(
            "<KMSMasterKeyID>arn:aws:kms:us-east-1:123456789012:key/abc</KMSMasterKeyID>"
        ));
        assert!(xml.contains("<BucketKeyEnabled>true</BucketKeyEnabled>"));
    }

    #[test]
    fn encryption_aes256_defaults_bucket_key_false() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{
                        "ServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" }
                    }]
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.encryption_config.unwrap();
        assert!(xml.contains("<SSEAlgorithm>AES256</SSEAlgorithm>"));
        assert!(!xml.contains("KMSMasterKeyID"));
        assert!(xml.contains("<BucketKeyEnabled>false</BucketKeyEnabled>"));
    }

    #[test]
    fn public_access_block_all_true() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": true,
                    "BlockPublicPolicy": true,
                    "IgnorePublicAcls": true,
                    "RestrictPublicBuckets": true
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.public_access_block.unwrap();
        assert!(xml.contains("<BlockPublicAcls>true</BlockPublicAcls>"));
        assert!(xml.contains("<RestrictPublicBuckets>true</RestrictPublicBuckets>"));
    }

    #[test]
    fn public_access_block_accepts_string_bools() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({ "PublicAccessBlockConfiguration": { "BlockPublicAcls": "true" } }),
            &store(),
        )
        .unwrap();
        let xml = b.public_access_block.unwrap();
        assert!(xml.contains("<BlockPublicAcls>true</BlockPublicAcls>"));
    }

    #[test]
    fn notification_lambda_queue_topic_and_eventbridge() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "NotificationConfiguration": {
                    "LambdaConfigurations": [{
                        "Event": "s3:ObjectCreated:*",
                        "Function": "arn:aws:lambda:us-east-1:123456789012:function:f",
                        "Filter": { "S3Key": { "Rules": [{ "Name": "prefix", "Value": "in/" }] } }
                    }],
                    "QueueConfigurations": [{
                        "Event": "s3:ObjectRemoved:*",
                        "Queue": "arn:aws:sqs:us-east-1:123456789012:q"
                    }],
                    "TopicConfigurations": [{
                        "Event": "s3:ObjectCreated:Put",
                        "Topic": "arn:aws:sns:us-east-1:123456789012:t"
                    }],
                    "EventBridgeConfiguration": {}
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.notification_config.clone().unwrap();
        // Lambda uses the newer <LambdaFunctionConfiguration>/<Function> shape.
        assert!(xml.contains("<LambdaFunctionConfiguration>"));
        assert!(
            xml.contains("<Function>arn:aws:lambda:us-east-1:123456789012:function:f</Function>")
        );
        assert!(xml.contains("<Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>"));
        assert!(xml.contains("<Topic>arn:aws:sns:us-east-1:123456789012:t</Topic>"));
        assert!(xml.contains("<FilterRule><Name>prefix</Name><Value>in/</Value></FilterRule>"));
        // normalize_notification_ids injects an <Id> for each entry.
        assert!(xml.contains("<Id>"));
        assert!(b.eventbridge_enabled);

        // The stored config must parse into targets on the firing path.
        let targets = crate::service::notifications::parse_notification_config(&xml);
        assert_eq!(targets.len(), 3);
    }

    #[test]
    fn tags_from_list() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({ "Tags": [{ "Key": "env", "Value": "prod" }, { "Key": "team", "Value": "core" }] }),
            &store(),
        )
        .unwrap();
        assert_eq!(b.tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(b.tags.get("team").map(String::as_str), Some("core"));
    }

    #[test]
    fn website_index_and_error() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({ "WebsiteConfiguration": { "IndexDocument": "index.html", "ErrorDocument": "error.html" } }),
            &store(),
        )
        .unwrap();
        let xml = b.website_config.unwrap();
        assert!(xml.contains("<IndexDocument><Suffix>index.html</Suffix></IndexDocument>"));
        assert!(xml.contains("<ErrorDocument><Key>error.html</Key></ErrorDocument>"));
    }

    #[test]
    fn cors_rule_maps_field_names() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "CorsConfiguration": {
                    "CorsRules": [{
                        "AllowedMethods": ["GET", "PUT"],
                        "AllowedOrigins": ["*"],
                        "ExposedHeaders": ["ETag"],
                        "MaxAge": 3000,
                        "Id": "rule1"
                    }]
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.cors_config.unwrap();
        assert!(xml.contains("<AllowedMethod>GET</AllowedMethod>"));
        assert!(xml.contains("<AllowedMethod>PUT</AllowedMethod>"));
        assert!(xml.contains("<AllowedOrigin>*</AllowedOrigin>"));
        assert!(xml.contains("<ExposeHeader>ETag</ExposeHeader>"));
        assert!(xml.contains("<MaxAgeSeconds>3000</MaxAgeSeconds>"));
        assert!(xml.contains("<ID>rule1</ID>"));
    }

    #[test]
    fn lifecycle_rule_passes_validation() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "LifecycleConfiguration": {
                    "Rules": [{
                        "Id": "expire-logs",
                        "Status": "Enabled",
                        "Prefix": "logs/",
                        "ExpirationInDays": 30,
                        "Transitions": [{ "StorageClass": "GLACIER", "TransitionInDays": 7 }],
                        "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 3 }
                    }]
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.lifecycle_config.unwrap();
        assert!(xml.contains("<Filter><Prefix>logs/</Prefix></Filter>"));
        assert!(xml.contains("<Expiration><Days>30</Days></Expiration>"));
        assert!(xml.contains(
            "<Transition><StorageClass>GLACIER</StorageClass><Days>7</Days></Transition>"
        ));
        assert!(xml.contains("<DaysAfterInitiation>3</DaysAfterInitiation>"));
        // Must satisfy the same validation PutBucketLifecycleConfiguration runs.
        assert!(crate::service::validate_lifecycle_xml(&xml).is_ok());
    }

    #[test]
    fn logging_config_maps_destination() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({ "LoggingConfiguration": { "DestinationBucketName": "log-bkt", "LogFilePrefix": "s3/" } }),
            &store(),
        )
        .unwrap();
        let xml = b.logging_config.unwrap();
        assert!(xml.contains("<TargetBucket>log-bkt</TargetBucket>"));
        assert!(xml.contains("<TargetPrefix>s3/</TargetPrefix>"));
    }

    #[test]
    fn absent_properties_are_noops() {
        let mut b = bucket();
        apply_cfn_bucket_properties(&mut b, &json!({ "BucketName": "b" }), &store()).unwrap();
        assert!(b.versioning.is_none());
        assert!(b.encryption_config.is_none());
        assert!(b.public_access_block.is_none());
        assert!(b.notification_config.is_none());
        assert!(b.tags.is_empty());
    }
}
