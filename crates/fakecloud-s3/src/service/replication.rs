//! Live replication: parsing and validating a bucket's replication
//! configuration, choosing the rules that apply to a new object version, and
//! writing its replicas (and replicated delete markers) into the destination
//! buckets.
//!
//! Semantics follow the S3 User Guide:
//!
//! - "Replication configuration file elements"
//!   (<https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-add-config.html>):
//!   a rule applies to the objects its `Filter` selects (key prefix, tags, or
//!   both under `And`; an empty filter selects everything); a V1 rule (no
//!   `Filter`) filters on its rule-level `Prefix` only. "If there are 2 or
//!   more rules with the same destination bucket, then objects are replicated
//!   according to the rule with the highest priority." Replicas keep the
//!   source object's storage class unless the destination names one. SSE-KMS
//!   objects are replicated only by rules that opt in through
//!   `SourceSelectionCriteria/SseKmsEncryptedObjects`, with the replica
//!   encrypted under the destination's `ReplicaKmsKeyID`.
//! - "What does Amazon S3 replicate?"
//!   (<https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-what-is-isnot-replicated.html>):
//!   only objects created after the configuration was added, never replicas
//!   of another rule, never S3 Glacier Flexible Retrieval / Deep Archive
//!   objects; a delete marker is replicated by V1 rules and by V2 rules with
//!   delete marker replication enabled (never by tag-based rules), and a
//!   versioned DELETE is never replicated.
//! - "Requirements and considerations for replication"
//!   (<https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-requirements.html>):
//!   both buckets must be versioning-enabled. Versioning cannot be suspended
//!   on a source that has a replication configuration, and "if you disable
//!   versioning on the destination bucket, replication fails. The source
//!   object has the replication status FAILED."

use fakecloud_aws::arn::Arn;
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_persistence::{BodySource, S3Store, StoreError, StoreResult};
use http::StatusCode;

use fakecloud_core::service::AwsServiceError;

use crate::side_effects::S3SideEffects;
use crate::state::{S3Bucket, S3Object, S3State};

use super::notifications::extract_all_xml_values;
use super::{extract_xml_value, xml_escape};

/// One rule of a replication configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReplicationRule {
    pub(crate) id: Option<String>,
    pub(crate) status: String,
    /// `Priority`; `None` on a V1 rule.
    pub(crate) priority: Option<i64>,
    /// A V2 rule carries a `Filter`; a V1 rule filters on a rule-level
    /// `Prefix` only.
    pub(crate) v2: bool,
    pub(crate) prefix: String,
    /// Tags an object must carry (all of them) for the rule to apply.
    pub(crate) tags: Vec<(String, String)>,
    /// Whether the rule replicates delete markers: `DeleteMarkerReplication`
    /// `Enabled` on a V2 rule; always on a V1 rule.
    pub(crate) delete_marker_replication: bool,
    /// `SourceSelectionCriteria/SseKmsEncryptedObjects` is `Enabled`.
    pub(crate) sse_kms_encrypted_objects: bool,
    pub(crate) dest_bucket: String,
    /// `Destination.Account`: the account expected to own the destination
    /// bucket (required alongside `AccessControlTranslation`).
    pub(crate) dest_account: Option<String>,
    /// `Destination.AccessControlTranslation.Owner == Destination`: replicas
    /// are owned by the destination bucket owner instead of the source
    /// object's owner.
    pub(crate) owner_override: bool,
    /// `Destination.StorageClass`: the replicas' storage class.
    pub(crate) storage_class: Option<String>,
    /// `Destination.EncryptionConfiguration.ReplicaKmsKeyID`.
    pub(crate) replica_kms_key_id: Option<String>,
}

impl ReplicationRule {
    fn enabled(&self) -> bool {
        self.status == "Enabled"
    }

    /// Whether the rule's filter selects `key` with `tags`.
    fn selects(&self, key: &str, tags: &std::collections::BTreeMap<String, String>) -> bool {
        key.starts_with(&self.prefix)
            && self
                .tags
                .iter()
                .all(|(k, v)| tags.get(k).is_some_and(|t| t == v))
    }
}

/// The `<Rule>` bodies of a replication configuration, in document order.
fn rule_bodies(xml: &str) -> Vec<&str> {
    let mut bodies = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find("<Rule>") {
        let after = &remaining[start + "<Rule>".len()..];
        let Some(end) = after.find("</Rule>") else {
            break;
        };
        bodies.push(&after[..end]);
        remaining = &after[end + "</Rule>".len()..];
    }
    bodies
}

/// The contents of the first `<tag>...</tag>` block in `body` (empty for a
/// self-closing `<tag/>`), or `None` when it is absent or malformed.
fn block<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    if let Some(start) = body.find(&open) {
        let inner = start + open.len();
        let end = body[inner..].find(&close)? + inner;
        return Some(&body[inner..end]);
    }
    if body.contains(&format!("<{tag}/>")) || body.contains(&format!("<{tag} />")) {
        return Some("");
    }
    None
}

/// `body` with every `<tag>...</tag>` (or self-closing) block named in `tags`
/// removed, leaving only the rule's own top-level elements. Several nested
/// elements carry their own `Status` / `Prefix`, which must not be mistaken
/// for the rule's.
fn strip_blocks(body: &str, tags: &[&str]) -> String {
    let mut out = body.to_string();
    for tag in tags {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        while let Some(start) = out.find(&open) {
            let Some(rel_end) = out[start..].find(&close) else {
                break;
            };
            out.replace_range(start..start + rel_end + close.len(), "");
        }
        for self_closing in [format!("<{tag}/>"), format!("<{tag} />")] {
            out = out.replace(&self_closing, "");
        }
    }
    out
}

const NESTED_RULE_ELEMENTS: &[&str] = &[
    "Filter",
    "Destination",
    "DeleteMarkerReplication",
    "SourceSelectionCriteria",
    "ExistingObjectReplication",
];

fn status_enabled(block_body: Option<&str>) -> bool {
    block_body
        .and_then(|b| extract_xml_value(b, "Status"))
        .is_some_and(|s| s == "Enabled")
}

fn parse_rule(body: &str) -> ReplicationRule {
    let top = strip_blocks(body, NESTED_RULE_ELEMENTS);
    let filter = block(body, "Filter");
    let v2 = filter.is_some();
    let (prefix, tags) = match filter {
        Some(filter) => {
            let scope = block(filter, "And").unwrap_or(filter);
            let prefix =
                extract_xml_value(&strip_blocks(scope, &["Tag"]), "Prefix").unwrap_or_default();
            let keys = extract_all_xml_values(scope, "Key");
            let values = extract_all_xml_values(scope, "Value");
            (prefix, keys.into_iter().zip(values).collect())
        }
        None => (
            extract_xml_value(&top, "Prefix").unwrap_or_default(),
            Vec::new(),
        ),
    };
    let destination = block(body, "Destination").unwrap_or("");
    let dest_bucket = extract_xml_value(destination, "Bucket")
        // ARN format: arn:<partition>:s3:::bucket-name
        .map(|arn| arn.rsplit(":::").next().unwrap_or(&arn).to_string())
        .unwrap_or_default();
    ReplicationRule {
        id: extract_xml_value(&top, "ID"),
        status: extract_xml_value(&top, "Status").unwrap_or_else(|| "Enabled".to_string()),
        priority: extract_xml_value(&top, "Priority").and_then(|p| p.trim().parse().ok()),
        v2,
        prefix,
        tags,
        delete_marker_replication: if v2 {
            status_enabled(block(body, "DeleteMarkerReplication"))
        } else {
            true
        },
        sse_kms_encrypted_objects: status_enabled(
            block(body, "SourceSelectionCriteria")
                .and_then(|ssc| block(ssc, "SseKmsEncryptedObjects")),
        ),
        dest_account: extract_xml_value(destination, "Account").filter(|a| !a.is_empty()),
        owner_override: block(destination, "AccessControlTranslation")
            .and_then(|act| extract_xml_value(act, "Owner"))
            .is_some_and(|owner| owner == "Destination"),
        storage_class: extract_xml_value(destination, "StorageClass").filter(|c| !c.is_empty()),
        replica_kms_key_id: block(destination, "EncryptionConfiguration")
            .and_then(|enc| extract_xml_value(enc, "ReplicaKmsKeyID"))
            .filter(|k| !k.is_empty()),
        dest_bucket,
    }
}

/// Parse a replication configuration's rules, in document order. Rules
/// without a destination bucket are dropped.
pub(crate) fn parse_replication_rules(xml: &str) -> Vec<ReplicationRule> {
    rule_bodies(xml)
        .into_iter()
        .map(parse_rule)
        .filter(|r| !r.dest_bucket.is_empty())
        .collect()
}

fn invalid_request(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequest", message)
}

/// Reject a replication configuration S3 refuses at PutBucketReplication:
///
/// - two rules with the same `Priority`: the priority is what decides which
///   rule applies when several select the same object for one destination,
///   so it must be unique among the rules;
/// - a rule opting in to SSE-KMS objects (`SourceSelectionCriteria`
///   `SseKmsEncryptedObjects` `Enabled`) without the destination's
///   `EncryptionConfiguration` `ReplicaKmsKeyID` to encrypt replicas under;
/// - a tag-based rule with delete marker replication enabled ("If your
///   `Filter` includes a `Tag` element, the `DeleteMarkerReplication`
///   `Status` must be set to Disabled", API Reference, `ReplicationRule`).
pub(crate) fn validate_replication_config(xml: &str) -> Result<(), AwsServiceError> {
    let rules: Vec<ReplicationRule> = rule_bodies(xml).into_iter().map(parse_rule).collect();
    let mut seen = std::collections::BTreeSet::new();
    for rule in &rules {
        if let Some(p) = rule.priority {
            if !seen.insert(p) {
                return Err(invalid_request(format!(
                    "Found two rules with the same priority ({p}). Each rule's priority must \
                     be unique."
                )));
            }
        }
        if rule.sse_kms_encrypted_objects && rule.replica_kms_key_id.is_none() {
            return Err(invalid_request(
                "ReplicaKmsKeyID must be specified if SseKmsEncryptedObjects tag is present.",
            ));
        }
        if rule.v2 && !rule.tags.is_empty() && rule.delete_marker_replication {
            return Err(invalid_request(
                "Delete marker replication is not supported if any Tag filter is specified.",
            ));
        }
    }
    Ok(())
}

/// Fill in what S3 stores for a rule the request left out: every rule gets an
/// `ID`, and a V2 rule (one with a `Filter`) gets a `Priority` and a disabled
/// `DeleteMarkerReplication`. A V1 rule is kept in the V1 shape -- its
/// rule-level `Prefix` is its filter -- and everything else in a rule
/// (`SourceSelectionCriteria`, the full `Destination`, ...) is kept as sent.
///
/// A missing priority takes the lowest positive value no other rule uses, so
/// it never collides with one the request set.
pub(crate) fn normalize_replication_xml(xml: &str) -> String {
    let bodies = rule_bodies(xml);
    if bodies.is_empty() {
        return xml.to_string();
    }
    let mut taken: std::collections::BTreeSet<i64> = bodies
        .iter()
        .filter_map(|b| parse_rule(b).priority)
        .collect();
    let mut next_priority = 1;

    let mut result = String::with_capacity(xml.len() + 128);
    let mut remaining = xml;
    while let Some(start) = remaining.find("<Rule>") {
        let after = &remaining[start + "<Rule>".len()..];
        let Some(end) = after.find("</Rule>") else {
            break;
        };
        result.push_str(&remaining[..start]);
        let body = &after[..end];
        let rule = parse_rule(body);
        let top = strip_blocks(body, NESTED_RULE_ELEMENTS);
        result.push_str("<Rule>");
        if extract_xml_value(&top, "ID").is_none() {
            result.push_str(&format!(
                "<ID>{}</ID>",
                xml_escape(&uuid::Uuid::new_v4().to_string())
            ));
        }
        if rule.v2 {
            if rule.priority.is_none() {
                while taken.contains(&next_priority) {
                    next_priority += 1;
                }
                taken.insert(next_priority);
                result.push_str(&format!("<Priority>{next_priority}</Priority>"));
            }
            if block(body, "DeleteMarkerReplication").is_none() {
                result.push_str(
                    "<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>",
                );
            }
        }
        if extract_xml_value(&top, "Status").is_none() {
            result.push_str("<Status>Enabled</Status>");
        }
        result.push_str(body);
        result.push_str("</Rule>");
        remaining = &after[end + "</Rule>".len()..];
    }
    result.push_str(remaining);
    result
}

/// The rules that apply, one per destination bucket: among the enabled rules
/// `eligible` accepts, the highest-priority rule for each destination (the
/// first in document order on a tie, which only rules without a priority can
/// produce). Destinations keep the order their first rule appears in.
fn winning_rules(
    rules: &[ReplicationRule],
    eligible: impl Fn(&ReplicationRule) -> bool,
) -> Vec<&ReplicationRule> {
    let mut chosen: Vec<&ReplicationRule> = Vec::new();
    for rule in rules.iter().filter(|r| r.enabled() && eligible(r)) {
        match chosen
            .iter_mut()
            .find(|c| c.dest_bucket == rule.dest_bucket)
        {
            Some(current) => {
                if rule.priority.unwrap_or(0) > current.priority.unwrap_or(0) {
                    *current = rule;
                }
            }
            None => chosen.push(rule),
        }
    }
    chosen
}

/// The rules that replicate `obj` (a new version just committed to the
/// source), one per destination, or none when S3 does not replicate the
/// object at all:
///
/// - a version with no real id: the bucket's null version. Replication only
///   applies to versioning-enabled sources and copies the versions written
///   after it was configured; a null version is either an object written
///   before versioning was enabled (an existing object, which live
///   replication never copies) or one written while versioning was
///   suspended, which S3 does not allow on a source with a replication
///   configuration;
/// - a replica created by another replication rule;
/// - an object in S3 Glacier Flexible Retrieval or S3 Glacier Deep Archive.
pub(crate) fn rules_for_object<'a>(
    rules: &'a [ReplicationRule],
    obj: &S3Object,
) -> Vec<&'a ReplicationRule> {
    if crate::service::is_null_version(obj)
        || obj.is_delete_marker
        || obj.replication_status.as_deref() == Some("REPLICA")
        || matches!(obj.storage_class.as_str(), "GLACIER" | "DEEP_ARCHIVE")
    {
        return Vec::new();
    }
    // The opt-in covers SSE-KMS and DSSE-KMS objects alike ("By default,
    // Amazon S3 doesn't replicate objects that are encrypted by using SSE-KMS
    // or DSSE-KMS"), so this is a prefix match, unlike the envelope check.
    let kms_encrypted = obj
        .sse_algorithm
        .as_deref()
        .is_some_and(|a| a.starts_with("aws:kms"));
    // The rule that applies to a destination is chosen by filter and
    // priority alone; its own criteria (the SSE-KMS opt-in) then decide
    // whether the object goes there. A lower-priority rule never stands in
    // for a winning rule that declines the object.
    winning_rules(rules, |r| r.selects(&obj.key, &obj.tags))
        .into_iter()
        .filter(|r| !kms_encrypted || r.sse_kms_encrypted_objects)
        .collect()
}

/// The rules that replicate a delete marker for `key`, one per destination:
/// a marker carries no tags, so tag-based rules never select it, and the
/// winning rule for a destination must replicate delete markers.
pub(crate) fn rules_for_delete_marker<'a>(
    rules: &'a [ReplicationRule],
    key: &str,
) -> Vec<&'a ReplicationRule> {
    let no_tags = std::collections::BTreeMap::new();
    winning_rules(rules, |r| r.selects(key, &no_tags))
        .into_iter()
        .filter(|r| r.delete_marker_replication)
        .collect()
}

/// Resolve and authorize the destination bucket of `rule` for a replication
/// of `key` that needs `actions` on it, returning the owning account. `None`
/// when the rule cannot replicate into it (the replication fails).
#[allow(clippy::too_many_arguments)]
fn resolve_destination(
    accounts: &MultiAccountState<S3State>,
    source_account: &str,
    source_bucket: &str,
    rule: &ReplicationRule,
    key: &str,
    role: Option<&str>,
    iam_mode: fakecloud_core::auth::IamMode,
    actions: &[&str],
) -> Option<String> {
    let dest_account = replication_dest_account(accounts, source_account, rule)?;
    if dest_account != source_account && iam_mode.is_enabled() {
        let allowed = replication_authorized(
            accounts,
            &dest_account,
            &rule.dest_bucket,
            key,
            source_account,
            role,
            actions,
        );
        if !allowed {
            tracing::warn!(
                target: "fakecloud::iam::audit",
                source_bucket,
                destination_bucket = %rule.dest_bucket,
                role = role.unwrap_or(""),
                mode = %iam_mode,
                "S3 replication: destination bucket policy does not allow the replication role"
            );
            if iam_mode.is_strict() {
                return None;
            }
        }
    }
    // Replication needs versioning on the destination too; S3 fails the
    // replication of a version whose destination has it disabled.
    let versioned = accounts
        .get(&dest_account)
        .and_then(|s| s.buckets.get(&rule.dest_bucket))
        .is_some_and(|b| b.versioning.as_deref() == Some("Enabled"));
    if !versioned {
        tracing::warn!(
            source_bucket,
            destination_bucket = %rule.dest_bucket,
            "S3 replication failed: the destination bucket is not versioning-enabled"
        );
        return None;
    }
    Some(dest_account)
}

/// Replicate the version of `key` just committed to `source_bucket` to the
/// destinations of the rules that apply to it (see [`rules_for_object`]), and
/// persist each replica through `store`.
///
/// Bucket names are global, so a destination bucket is looked up first in the
/// source bucket's account and then across every account: a rule may
/// replicate into a bucket another account owns. When the rule names a
/// `Destination.Account` that does not own the destination bucket, AWS fails
/// the replication, so nothing is written.
///
/// Writing into another account's bucket needs that bucket's policy to allow
/// the replication role (`s3:ReplicateObject`, plus
/// `s3:ObjectOwnerOverrideToBucketOwner` with ownership translation). Like
/// every cross-account check, this follows the IAM mode in `effects`: a
/// refusal is logged under `soft` and enforced under `strict`.
///
/// The source version's `x-amz-replication-status` becomes `COMPLETED` when
/// every applicable rule replicated it and `FAILED` otherwise (S3 reports
/// `PENDING` until it finishes; here replication finishes with the write);
/// replicas are marked `REPLICA`.
///
/// The source write has already succeeded when this runs, so nothing here
/// fails it: a replica that cannot be read, encrypted or stored fails that
/// replication, which the source reports as `FAILED`, as on AWS.
///
/// It runs under the S3 write guard that committed the source, so a newer
/// version of the key cannot be replicated ahead of this one (the
/// destination history keeps the source's order). That includes re-encrypting
/// an SSE-KMS replica: an in-memory envelope operation through the KMS hook,
/// with no disk or network IO.
pub(crate) fn replicate_through_store(
    accounts: &mut MultiAccountState<S3State>,
    source_account: &str,
    store: Option<&dyn S3Store>,
    source_bucket: &str,
    key: &str,
    effects: Option<&S3SideEffects>,
) {
    let Some(state) = accounts.get(source_account) else {
        return;
    };
    let Some(src_bucket) = state.buckets.get(source_bucket) else {
        return;
    };
    let Some(replication_config) = src_bucket.replication_config.clone() else {
        return;
    };
    let Some(src_obj) = src_bucket.objects.get(key).cloned() else {
        return;
    };
    let rules = parse_replication_rules(&replication_config);
    let applicable = rules_for_object(&rules, &src_obj);
    if applicable.is_empty() {
        return;
    }
    let role = extract_xml_value(&replication_config, "Role");
    let iam_mode = effects.map(|e| e.iam_mode).unwrap_or_default();
    let kms_hook = effects.and_then(|e| e.kms_hook.as_ref());

    // A disk-backed source is copied file-to-file; a memory one is read once.
    // An SSE-KMS source re-encrypted for its replica needs its plaintext.
    let src_disk_path = match &src_obj.body {
        fakecloud_persistence::BodyRef::Disk { path, .. } => Some(path.clone()),
        fakecloud_persistence::BodyRef::Memory(_) => None,
    };
    let mut src_bytes: Option<bytes::Bytes> = None;
    let mut read_source = |accounts: &MultiAccountState<S3State>| -> StoreResult<bytes::Bytes> {
        if let Some(b) = &src_bytes {
            return Ok(b.clone());
        }
        let state = accounts
            .get(source_account)
            .ok_or_else(|| StoreError::Other("source account vanished".to_string()))?;
        let b = state.read_body(&src_obj.body).map_err(StoreError::Io)?;
        src_bytes = Some(b.clone());
        Ok(b)
    };
    let replication_failed = |what: &str, err: &dyn std::fmt::Display, dest: &str| {
        tracing::warn!(
            source_bucket,
            destination_bucket = dest,
            error = %err,
            "S3 replication failed: {what}"
        );
    };

    let mut failed = false;
    for rule in applicable {
        let Some(dest_account) = resolve_destination(
            accounts,
            source_account,
            source_bucket,
            rule,
            key,
            role.as_deref(),
            iam_mode,
            if rule.owner_override {
                &["s3:ReplicateObject", "s3:ObjectOwnerOverrideToBucketOwner"]
            } else {
                &["s3:ReplicateObject"]
            },
        ) else {
            failed = true;
            continue;
        };
        let dest_region = accounts
            .get(&dest_account)
            .and_then(|s| s.buckets.get(&rule.dest_bucket))
            .map(|b| b.region.clone())
            .unwrap_or_default();

        let mut replica = src_obj.clone();
        // The replica keeps the source version's id, metadata, tags,
        // checksum and lock (a source version that carries no lock takes the
        // destination's default retention at commit). It is created now, in
        // the destination: its LastModified (which also orders versions on
        // reload) is the commit time, not the source's.
        replica.replication_status = Some("REPLICA".to_string());
        replica.last_modified = chrono::Utc::now();
        if let Some(class) = &rule.storage_class {
            replica.storage_class = class.clone();
        }

        // An SSE-KMS replica is encrypted under the destination's replica
        // key: decrypt the source envelope and encrypt it again.
        let reencrypt = rule.replica_kms_key_id.as_deref().filter(|_| {
            crate::side_effects::is_sse_kms(src_obj.sse_algorithm.as_deref()) && kms_hook.is_some()
        });
        let body_source = if let Some(replica_key) = reencrypt {
            let stored = match read_source(accounts) {
                Ok(b) => b,
                Err(e) => {
                    replication_failed("the source could not be read", &e, &rule.dest_bucket);
                    failed = true;
                    continue;
                }
            };
            let plaintext = crate::side_effects::kms_decrypt(
                kms_hook,
                source_account,
                source_bucket,
                &src_obj.key,
                src_obj.bucket_key_enabled == Some(true),
                &stored,
                src_obj.sse_kms_key_id.as_deref(),
            );
            let encrypted = plaintext.and_then(|p| {
                crate::side_effects::kms_encrypt(
                    kms_hook,
                    &dest_account,
                    &dest_region,
                    &rule.dest_bucket,
                    &replica.key,
                    replica.bucket_key_enabled == Some(true),
                    &p,
                    Some(replica_key),
                )
            });
            match encrypted {
                Ok(cipher) => {
                    replica.sse_kms_key_id = Some(
                        crate::side_effects::resolve_kms_key_arn(
                            kms_hook,
                            &dest_account,
                            &dest_region,
                            replica_key,
                        )
                        .unwrap_or_else(|| replica_key.to_string()),
                    );
                    replica.body = crate::state::memory_body(cipher.clone());
                    BodySource::Bytes(cipher)
                }
                Err(err) => {
                    replication_failed(
                        "the replica could not be encrypted",
                        &err,
                        &rule.dest_bucket,
                    );
                    failed = true;
                    continue;
                }
            }
        } else if let Some(path) = &src_disk_path {
            BodySource::FileCopy(path.clone())
        } else {
            let bytes = match read_source(accounts) {
                Ok(b) => b,
                Err(e) => {
                    replication_failed("the source could not be read", &e, &rule.dest_bucket);
                    failed = true;
                    continue;
                }
            };
            replica.body = crate::state::memory_body(bytes.clone());
            BodySource::Bytes(bytes)
        };

        let Some(dest_bucket) = accounts
            .get_mut(&dest_account)
            .and_then(|s| s.buckets.get_mut(&rule.dest_bucket))
        else {
            failed = true;
            continue;
        };
        translate_replica_owner(&mut replica, dest_bucket, rule.owner_override);
        // A failed store write leaves the destination untouched (the commit
        // records nothing it could not persist).
        if let Err(e) = crate::service::commit_object_version(
            dest_bucket,
            &rule.dest_bucket,
            store,
            replica,
            body_source,
        ) {
            replication_failed("the replica could not be stored", &e, &rule.dest_bucket);
            failed = true;
        }
    }
    let status = if failed { "FAILED" } else { "COMPLETED" };
    mark_source_replication_status(
        accounts,
        source_account,
        store,
        source_bucket,
        key,
        src_obj.version_id.as_deref(),
        status,
    );
}

/// Replicate the delete marker `marker_version_id` just added for `key` in
/// `source_bucket` to the destinations of the rules that replicate delete
/// markers for it (see [`rules_for_delete_marker`]). The replicated marker
/// keeps the source marker's version id. A destination that cannot take it
/// (missing, not versioning-enabled, or refusing `s3:ReplicateDelete` to
/// another account's replication role under strict IAM, or failing to store
/// it) is skipped: a delete marker has no replication status to record the
/// failure on, and the source delete itself has already succeeded.
pub(crate) fn replicate_delete_marker(
    accounts: &mut MultiAccountState<S3State>,
    source_account: &str,
    store: Option<&dyn S3Store>,
    source_bucket: &str,
    key: &str,
    marker_version_id: &str,
    effects: Option<&S3SideEffects>,
) {
    let Some(replication_config) = accounts
        .get(source_account)
        .and_then(|s| s.buckets.get(source_bucket))
        .and_then(|b| b.replication_config.clone())
    else {
        return;
    };
    let rules = parse_replication_rules(&replication_config);
    let applicable = rules_for_delete_marker(&rules, key);
    if applicable.is_empty() {
        return;
    }
    let role = extract_xml_value(&replication_config, "Role");
    let iam_mode = effects.map(|e| e.iam_mode).unwrap_or_default();
    for rule in applicable {
        let Some(dest_account) = resolve_destination(
            accounts,
            source_account,
            source_bucket,
            rule,
            key,
            role.as_deref(),
            iam_mode,
            &["s3:ReplicateDelete"],
        ) else {
            continue;
        };
        let Some(dest_bucket) = accounts
            .get_mut(&dest_account)
            .and_then(|s| s.buckets.get_mut(&rule.dest_bucket))
        else {
            continue;
        };
        let marker = crate::service::make_delete_marker(key, marker_version_id);
        if let Err(e) = crate::service::commit_object_version(
            dest_bucket,
            &rule.dest_bucket,
            store,
            marker,
            BodySource::Bytes(bytes::Bytes::new()),
        ) {
            tracing::warn!(
                source_bucket,
                destination_bucket = %rule.dest_bucket,
                error = %e,
                "S3 delete marker replication failed: the marker could not be stored"
            );
        }
    }
}

/// Record `status` as the source object's `x-amz-replication-status` (the
/// current object and its entry in the version history) and persist it.
fn mark_source_replication_status(
    accounts: &mut MultiAccountState<S3State>,
    source_account: &str,
    store: Option<&dyn S3Store>,
    source_bucket: &str,
    key: &str,
    version_id: Option<&str>,
    status: &str,
) {
    let Some(bucket) = accounts
        .get_mut(source_account)
        .and_then(|s| s.buckets.get_mut(source_bucket))
    else {
        return;
    };
    let mut meta = None;
    if let Some(obj) = bucket.objects.get_mut(key) {
        if obj.version_id.as_deref() == version_id {
            obj.replication_status = Some(status.to_string());
            meta = Some(crate::persistence::object_meta_snapshot(obj));
        }
    }
    if let Some(versions) = bucket.object_versions.get_mut(key) {
        if let Some(v) = versions
            .iter_mut()
            .rev()
            .find(|v| v.version_id.as_deref() == version_id)
        {
            v.replication_status = Some(status.to_string());
        }
    }
    // The source write already succeeded; a status that cannot be persisted
    // is logged (memory keeps it) rather than failing that write.
    if let (Some(meta), Some(store)) = (meta, store) {
        if let Err(e) = store.put_object_meta(source_bucket, key, version_id, &meta) {
            tracing::warn!(
                source_bucket,
                key,
                error = %e,
                "failed to persist the replication status of a source object"
            );
        }
    }
}

/// Whether the destination bucket's policy lets the replication role
/// (`role`, in `source_account`; the account root when the configuration
/// names none) perform every one of `actions` on `key` in it. A bucket
/// without a policy grants nothing to another account.
fn replication_authorized(
    accounts: &MultiAccountState<S3State>,
    dest_account: &str,
    dest_bucket: &str,
    key: &str,
    source_account: &str,
    role: Option<&str>,
    actions: &[&str],
) -> bool {
    let Some(bucket) = accounts
        .get(dest_account)
        .and_then(|s| s.buckets.get(dest_bucket))
    else {
        return false;
    };
    let Some(policy) = bucket.policy.as_deref() else {
        return false;
    };
    let doc = fakecloud_iam::evaluator::PolicyDocument::parse(policy);
    let partition = fakecloud_aws::arn::partition_for(&bucket.region);
    let principal = match role.filter(|r| !r.is_empty()) {
        Some(role) => fakecloud_core::auth::Principal {
            arn: role.to_string(),
            user_id: role.to_string(),
            account_id: source_account.to_string(),
            principal_type: fakecloud_core::auth::PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        },
        None => fakecloud_core::auth::Principal {
            arn: Arn::global("iam", source_account, "root")
                .with_partition(partition)
                .to_string(),
            user_id: source_account.to_string(),
            account_id: source_account.to_string(),
            principal_type: fakecloud_core::auth::PrincipalType::Root,
            source_identity: None,
            tags: None,
        },
    };
    let resource = Arn::s3_in(&bucket.region, &format!("{dest_bucket}/{key}")).to_string();
    actions.iter().all(|action| {
        let request = fakecloud_iam::evaluator::EvalRequest {
            principal: &principal,
            action: action.to_string(),
            resource: resource.clone(),
            context: fakecloud_iam::evaluator::RequestContext {
                aws_principal_arn: Some(principal.arn.clone()),
                aws_principal_account: Some(principal.account_id.clone()),
                ..Default::default()
            },
        };
        matches!(
            fakecloud_iam::evaluator::evaluate_resource_policy_only(&doc, &request),
            fakecloud_iam::evaluator::Decision::Allow
        )
    })
}

/// The account owning a replication rule's destination bucket: the source
/// account when it has the bucket, otherwise whichever account does (bucket
/// names are global). `None` when no account owns it, or when the rule's
/// `Destination.Account` names a different owner (AWS fails that
/// replication).
fn replication_dest_account(
    accounts: &MultiAccountState<S3State>,
    source_account: &str,
    rule: &ReplicationRule,
) -> Option<String> {
    let bucket = rule.dest_bucket.as_str();
    let owner = if accounts
        .get(source_account)
        .is_some_and(|s| s.buckets.contains_key(bucket))
    {
        source_account.to_string()
    } else {
        accounts
            .find_account(|s| s.buckets.contains_key(bucket))?
            .to_string()
    };
    if let Some(expected) = rule.dest_account.as_deref() {
        if expected != owner {
            tracing::warn!(
                bucket,
                expected_account = expected,
                owner_account = %owner,
                "S3 replication: destination bucket is not owned by Destination.Account; replication failed"
            );
            return None;
        }
    }
    Some(owner)
}

/// Replicas keep the source object's owner and ACL unless the rule translates
/// ownership to the destination bucket owner (`AccessControlTranslation`
/// `Owner=Destination`) or the destination bucket enforces bucket-owner
/// ownership (ACLs disabled); either way the destination owner then owns the
/// replica with full control.
fn translate_replica_owner(replica: &mut S3Object, dest_bucket: &S3Bucket, owner_override: bool) {
    let owner_enforced = dest_bucket
        .ownership_controls
        .as_deref()
        .is_some_and(|c| c.contains("BucketOwnerEnforced"));
    if !owner_override && !owner_enforced {
        return;
    }
    let owner = dest_bucket.acl_owner_id.clone();
    replica.acl_grants = vec![crate::state::AclGrant {
        grantee_type: "CanonicalUser".to_string(),
        grantee_id: Some(owner.clone()),
        grantee_display_name: Some(owner.clone()),
        grantee_uri: None,
        permission: "FULL_CONTROL".to_string(),
    }];
    replica.acl_owner_id = Some(owner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn rule(dest: &str, priority: Option<i64>, prefix: &str) -> ReplicationRule {
        ReplicationRule {
            status: "Enabled".to_string(),
            priority,
            v2: true,
            prefix: prefix.to_string(),
            dest_bucket: dest.to_string(),
            ..Default::default()
        }
    }

    fn object(key: &str) -> S3Object {
        S3Object {
            key: key.to_string(),
            version_id: Some("v1".to_string()),
            storage_class: "STANDARD".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn parses_a_v2_rule_with_prefix_tags_and_options() {
        let xml = "<ReplicationConfiguration><Role>r</Role><Rule><ID>tagged</ID>\
            <Priority>7</Priority>\
            <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
            <Filter><And><Prefix>tax/</Prefix>\
            <Tag><Key>team</Key><Value>finance</Value></Tag>\
            <Tag><Key>tier</Key><Value>gold</Value></Tag></And></Filter>\
            <SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status>\
            </SseKmsEncryptedObjects></SourceSelectionCriteria>\
            <Destination><Bucket>arn:aws:s3:::dst</Bucket><StorageClass>STANDARD_IA</StorageClass>\
            <EncryptionConfiguration><ReplicaKmsKeyID>replica-key</ReplicaKmsKeyID>\
            </EncryptionConfiguration>\
            <Metrics><Status>Disabled</Status></Metrics></Destination>\
            <Status>Enabled</Status></Rule></ReplicationConfiguration>";
        let rules = parse_replication_rules(xml);
        assert_eq!(rules.len(), 1);
        let r = &rules[0];
        assert_eq!(r.id.as_deref(), Some("tagged"));
        // The rule's own Status, not a nested one.
        assert_eq!(r.status, "Enabled");
        assert_eq!(r.priority, Some(7));
        assert!(r.v2);
        assert_eq!(r.prefix, "tax/");
        assert_eq!(
            r.tags,
            vec![
                ("team".to_string(), "finance".to_string()),
                ("tier".to_string(), "gold".to_string())
            ]
        );
        assert!(!r.delete_marker_replication);
        assert!(r.sse_kms_encrypted_objects);
        assert_eq!(r.dest_bucket, "dst");
        assert_eq!(r.storage_class.as_deref(), Some("STANDARD_IA"));
        assert_eq!(r.replica_kms_key_id.as_deref(), Some("replica-key"));
    }

    #[test]
    fn parses_a_v1_rule_by_its_rule_level_prefix() {
        let xml = "<ReplicationConfiguration><Rule><Status>Disabled</Status>\
            <Prefix>logs/</Prefix>\
            <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
            </ReplicationConfiguration>";
        let r = &parse_replication_rules(xml)[0];
        assert!(!r.v2);
        assert_eq!(r.prefix, "logs/");
        assert_eq!(r.status, "Disabled");
        assert_eq!(r.priority, None);
        // V1 replicates delete markers.
        assert!(r.delete_marker_replication);
    }

    #[test]
    fn an_empty_filter_selects_everything() {
        let xml = "<ReplicationConfiguration><Rule><Status>Enabled</Status><Filter/>\
            <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination></Rule>\
            </ReplicationConfiguration>";
        let r = &parse_replication_rules(xml)[0];
        assert!(r.v2);
        assert_eq!(r.prefix, "");
        assert!(r.tags.is_empty());
    }

    #[test]
    fn normalize_keeps_v1_rules_and_completes_v2_rules() {
        let xml = "<ReplicationConfiguration><Role>r</Role>\
            <Rule><Status>Enabled</Status><Prefix>logs/</Prefix>\
            <Destination><Bucket>arn:aws:s3:::a</Bucket></Destination></Rule>\
            <Rule><Status>Enabled</Status><Filter><Prefix>x/</Prefix></Filter>\
            <SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status>\
            </SseKmsEncryptedObjects></SourceSelectionCriteria>\
            <Destination><Bucket>arn:aws:s3:::b</Bucket></Destination></Rule>\
            <Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Prefix>y/</Prefix></Filter>\
            <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>\
            <Destination><Bucket>arn:aws:s3:::c</Bucket></Destination></Rule>\
            </ReplicationConfiguration>";
        let normalized = normalize_replication_xml(xml);
        let rules = parse_replication_rules(&normalized);
        assert_eq!(rules.len(), 3);
        // V1 stays V1: its prefix still filters, no priority or filter added.
        assert!(!rules[0].v2);
        assert_eq!(rules[0].prefix, "logs/");
        assert_eq!(rules[0].priority, None);
        assert!(rules[0].id.is_some());
        // A V2 rule without a priority gets one that does not collide with
        // the explicit 1, a disabled DeleteMarkerReplication, and keeps its
        // SourceSelectionCriteria.
        assert_eq!(rules[1].priority, Some(2));
        assert!(!rules[1].delete_marker_replication);
        assert!(rules[1].sse_kms_encrypted_objects);
        assert!(normalized.contains("<DeleteMarkerReplication><Status>Disabled</Status>"));
        assert_eq!(rules[2].priority, Some(1));
        assert!(rules[2].delete_marker_replication);
        // Normalizing again changes nothing.
        assert_eq!(normalize_replication_xml(&normalized), normalized);
    }

    #[test]
    fn validation_rejects_duplicate_priorities() {
        let xml = "<ReplicationConfiguration>\
            <Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Prefix>a</Prefix></Filter>\
            <Destination><Bucket>arn:aws:s3:::x</Bucket></Destination></Rule>\
            <Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Prefix>b</Prefix></Filter>\
            <Destination><Bucket>arn:aws:s3:::y</Bucket></Destination></Rule>\
            </ReplicationConfiguration>";
        let err = validate_replication_config(xml).unwrap_err();
        assert!(format!("{err:?}").contains("InvalidRequest"), "{err:?}");
        let ok = xml.replacen("<Priority>1</Priority>", "<Priority>2</Priority>", 1);
        assert!(validate_replication_config(&ok).is_ok());
    }

    #[test]
    fn validation_rejects_delete_marker_replication_on_tag_rules() {
        let xml = "<ReplicationConfiguration><Rule><Priority>1</Priority><Status>Enabled</Status>\
            <Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>\
            <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>\
            <Destination><Bucket>arn:aws:s3:::x</Bucket></Destination></Rule>\
            </ReplicationConfiguration>";
        assert!(validate_replication_config(xml).is_err());
        let disabled = xml.replace(
            "<DeleteMarkerReplication><Status>Enabled</Status>",
            "<DeleteMarkerReplication><Status>Disabled</Status>",
        );
        assert!(validate_replication_config(&disabled).is_ok());
    }

    /// Several rules selecting one object for the same destination: only the
    /// highest-priority one applies, so the object is replicated there once.
    /// Rules to different destinations all apply.
    #[test]
    fn one_rule_per_destination_the_highest_priority() {
        let rules = vec![
            rule("dst", Some(1), "star"),
            rule("dst", Some(5), "starship"),
            rule("dst", Some(3), ""),
            rule("other", Some(2), ""),
        ];
        let chosen = rules_for_object(&rules, &object("starship-x"));
        assert_eq!(chosen.len(), 2);
        assert_eq!(chosen[0].dest_bucket, "dst");
        assert_eq!(chosen[0].priority, Some(5));
        assert_eq!(chosen[1].dest_bucket, "other");

        // "star-y" is not selected by the priority-5 rule, so priority 3 wins.
        let chosen = rules_for_object(&rules, &object("star-y"));
        assert_eq!(chosen[0].priority, Some(3));
    }

    #[test]
    fn disabled_rules_and_unmatched_tags_do_not_apply() {
        let mut disabled = rule("dst", Some(9), "");
        disabled.status = "Disabled".to_string();
        let mut tagged = rule("dst2", Some(1), "");
        tagged.tags = vec![("team".to_string(), "a".to_string())];
        let rules = vec![disabled, tagged];
        assert!(rules_for_object(&rules, &object("k")).is_empty());

        let mut obj = object("k");
        obj.tags = BTreeMap::from([("team".to_string(), "a".to_string())]);
        let chosen = rules_for_object(&rules, &obj);
        assert_eq!(chosen.len(), 1);
        assert_eq!(chosen[0].dest_bucket, "dst2");
    }

    /// What S3 never replicates: a null version (written before versioning
    /// was enabled, or while it was suspended), a replica, and objects in the
    /// Glacier Flexible Retrieval / Deep Archive classes. SSE-KMS objects
    /// need a rule that opts in.
    #[test]
    fn objects_s3_does_not_replicate() {
        let rules = vec![rule("dst", Some(1), "")];
        for vid in [None, Some("null")] {
            let mut obj = object("k");
            obj.version_id = vid.map(str::to_string);
            assert!(rules_for_object(&rules, &obj).is_empty(), "{vid:?}");
        }
        let mut replica = object("k");
        replica.replication_status = Some("REPLICA".to_string());
        assert!(rules_for_object(&rules, &replica).is_empty());
        for class in ["GLACIER", "DEEP_ARCHIVE"] {
            let mut obj = object("k");
            obj.storage_class = class.to_string();
            assert!(rules_for_object(&rules, &obj).is_empty(), "{class}");
        }
        let mut ir = object("k");
        ir.storage_class = "GLACIER_IR".to_string();
        assert_eq!(rules_for_object(&rules, &ir).len(), 1);

        let mut kms = object("k");
        kms.sse_algorithm = Some("aws:kms".to_string());
        assert!(rules_for_object(&rules, &kms).is_empty());
        let mut opted_in = rule("dst", Some(1), "");
        opted_in.sse_kms_encrypted_objects = true;
        assert_eq!(rules_for_object(&[opted_in], &kms).len(), 1);
    }

    /// The winning rule for a destination is chosen by filter and priority
    /// before its SSE-KMS opt-in is consulted: a top-priority rule that does
    /// not opt in keeps an SSE-KMS object out of that destination even when
    /// a lower-priority rule to it would have replicated it.
    #[test]
    fn the_winning_rule_decides_sse_kms_objects() {
        let top = rule("dst", Some(5), "");
        let mut lower = rule("dst", Some(1), "");
        lower.sse_kms_encrypted_objects = true;
        let mut other = rule("other", Some(2), "");
        other.sse_kms_encrypted_objects = true;
        let rules = vec![top, lower, other];
        let mut kms = object("k");
        kms.sse_algorithm = Some("aws:kms".to_string());
        let chosen = rules_for_object(&rules, &kms);
        assert_eq!(chosen.len(), 1);
        assert_eq!(chosen[0].dest_bucket, "other");
        // An unencrypted object follows the top rule as usual.
        let chosen = rules_for_object(&rules, &object("k"));
        assert_eq!(chosen[0].priority, Some(5));
    }

    #[test]
    fn validation_requires_a_replica_key_for_sse_kms_objects() {
        let xml = "<ReplicationConfiguration><Rule><Priority>1</Priority><Status>Enabled</Status>\
            <Filter><Prefix></Prefix></Filter>\
            <SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status>\
            </SseKmsEncryptedObjects></SourceSelectionCriteria>\
            <Destination><Bucket>arn:aws:s3:::x</Bucket></Destination></Rule>\
            </ReplicationConfiguration>";
        let err = validate_replication_config(xml).unwrap_err();
        assert!(
            format!("{err:?}").contains("ReplicaKmsKeyID must be specified"),
            "{err:?}"
        );
        let with_key = xml.replace(
            "<Bucket>arn:aws:s3:::x</Bucket>",
            "<Bucket>arn:aws:s3:::x</Bucket><EncryptionConfiguration>\
             <ReplicaKmsKeyID>k</ReplicaKmsKeyID></EncryptionConfiguration>",
        );
        assert!(validate_replication_config(&with_key).is_ok());
    }

    /// The winning rule for a destination decides whether a delete marker
    /// goes there; tag-based rules never select a marker.
    #[test]
    fn delete_marker_rules() {
        let mut v2_on = rule("a", Some(2), "");
        v2_on.delete_marker_replication = true;
        let v2_off = rule("b", Some(1), "");
        let v1 = ReplicationRule {
            status: "Enabled".to_string(),
            v2: false,
            delete_marker_replication: true,
            prefix: "logs/".to_string(),
            dest_bucket: "c".to_string(),
            ..Default::default()
        };
        let mut tagged = rule("d", Some(3), "");
        tagged.tags = vec![("k".to_string(), "v".to_string())];
        tagged.delete_marker_replication = true;
        // A higher-priority rule to "a" with DMR off overrides the one with it on.
        let mut a_override = rule("a", Some(9), "keep/");
        a_override.delete_marker_replication = false;
        let rules = vec![v2_on, v2_off, v1, tagged, a_override];

        let dests = |key: &str| -> Vec<String> {
            rules_for_delete_marker(&rules, key)
                .into_iter()
                .map(|r| r.dest_bucket.clone())
                .collect()
        };
        assert_eq!(dests("logs/x"), vec!["a".to_string(), "c".to_string()]);
        assert_eq!(dests("other"), vec!["a".to_string()]);
        assert!(dests("keep/x").is_empty());
    }
}
