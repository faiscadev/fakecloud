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
use fakecloud_persistence::{
    AclGrantSnapshot, AclSnapshot, BucketSubresource, S3Store, StoreError, TagsSnapshot,
};

use crate::persistence::bucket_meta_snapshot;
use crate::service::notifications::normalize_notification_ids;
use crate::state::S3Bucket;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Subresource writes a property application staged, applied to the store
/// only once every property has validated: `Some` puts the payload, `None`
/// deletes the subresource. `meta_dirty` marks a change to a field that lives
/// in the bucket meta (versioning, EventBridge).
#[derive(Default)]
struct StagedWrites {
    subresources: Vec<(BucketSubresource, Option<String>)>,
    meta_dirty: bool,
}

impl StagedWrites {
    fn put(&mut self, kind: BucketSubresource, payload: String) {
        self.subresources.push((kind, Some(payload)));
    }

    fn persist(&self, store: &Arc<dyn S3Store>, bucket: &str) -> Result<(), String> {
        for (kind, payload) in &self.subresources {
            match payload {
                Some(body) => persist_sub(store, bucket, *kind, body)?,
                None => store
                    .delete_bucket_subresource(bucket, *kind)
                    .map_err(|e| persist_err(&format!("{kind:?}"), bucket, e))?,
            }
        }
        Ok(())
    }
}

/// Create the bucket an `AWS::S3::Bucket` resource describes: the settings S3
/// gives every new bucket (ACLs disabled, Block Public Access on, SSE-S3 with
/// SSE-C blocked) with the template's properties applied over them.
/// CloudFormation creates the bucket through CreateBucket, so a template that
/// sets none of these properties gets exactly what the API gives.
///
/// Every property validates before anything is written. The store then gets
/// the bucket meta first, the three create-time settings as the bucket holds
/// them (the routine CreateBucket uses), and the template's other
/// subresources. A write that fails partway removes everything this create
/// wrote, so no half-created bucket is left to load on the next restart.
pub fn apply_cfn_new_bucket(
    bucket: &mut S3Bucket,
    props: &Value,
    store: &Arc<dyn S3Store>,
) -> Result<(), String> {
    bucket.apply_new_bucket_defaults();
    let staged = stage_cfn_bucket_properties(bucket, props)?;
    let name = bucket.name.clone();
    // Same rules as CreateBucket for a name whose directory outlived its
    // bucket: data the loader refused is never cleared by a create, and any
    // other leftover (after a reset, or a create or delete that stopped
    // partway) is cleared so the new bucket does not inherit it.
    if store.bucket_load_refused(&name) && store.bucket_state_exists(&name) {
        return Err(format!(
            "The requested bucket name is not available: {name} holds persisted data that \
             could not be read at load. Repair it in the data path and restart, or remove the \
             directory to free the name. (Service: S3, Status Code: 409, Error Code: \
             BucketAlreadyExists)"
        ));
    }
    if store.bucket_state_exists(&name) {
        store
            .delete_bucket(&name)
            .map_err(|e| persist_err("leftover state", &name, e))?;
    }
    if store.bucket_load_refused(&name) {
        store
            .clear_bucket_load_refusal(&name)
            .map_err(|e| persist_err("load refusal", &name, e))?;
    }
    let written = (|| {
        store
            .put_bucket_meta(&name, &bucket_meta_snapshot(bucket))
            .map_err(|e| persist_err("meta", &name, e))?;
        for (kind, payload) in bucket.new_bucket_settings() {
            if let Some(body) = payload {
                persist_sub(store, &name, kind, body)?;
            }
        }
        // The create-time settings were just written from the bucket's final
        // state, so the template's own writes of them are redundant here.
        let settings = [
            BucketSubresource::Ownership,
            BucketSubresource::PublicAccessBlock,
            BucketSubresource::Encryption,
        ];
        let rest = StagedWrites {
            subresources: staged
                .subresources
                .iter()
                .filter(|(kind, _)| !settings.contains(kind))
                .cloned()
                .collect(),
            meta_dirty: false,
        };
        rest.persist(store, &name)
    })();
    if let Err(e) = written {
        if let Err(cleanup) = store.delete_bucket(&name) {
            tracing::warn!(
                bucket = %name,
                error = %cleanup,
                "could not remove the partly written state of a failed CloudFormation bucket create",
            );
        }
        return Err(e);
    }
    Ok(())
}

/// Whether `bucket`'s Block Public Access refuses `policy_json` as a public
/// bucket policy, which is what PutBucketPolicy checks and so what an
/// `AWS::S3::BucketPolicy` resource is held to as well.
pub fn bucket_policy_blocked(bucket: &S3Bucket, policy_json: &str) -> bool {
    bucket.public_access_block.as_deref().is_some_and(|xml| {
        crate::service::config::PublicAccessBlockFlags::parse(xml).block_public_policy
    }) && crate::service::config::policy_is_public(policy_json)
}

/// The canned ACL an `AccessControl` value names.
fn cfn_canned_acl(value: &str) -> Option<&'static str> {
    Some(match value {
        "Private" => "private",
        "PublicRead" => "public-read",
        "PublicReadWrite" => "public-read-write",
        "AuthenticatedRead" => "authenticated-read",
        "AwsExecRead" => "aws-exec-read",
        "LogDeliveryWrite" => "log-delivery-write",
        "BucketOwnerRead" => "bucket-owner-read",
        "BucketOwnerFullControl" => "bucket-owner-full-control",
        _ => return None,
    })
}

/// `OwnershipControls.Rules[0].ObjectOwnership`, validated.
fn cfn_object_ownership(v: &Value) -> Result<Option<String>, String> {
    if v.is_null() {
        return Ok(None);
    }
    let rules = v
        .get("Rules")
        .and_then(Value::as_array)
        .ok_or("OwnershipControls: Rules must be a list with one rule")?;
    let [rule] = rules.as_slice() else {
        return Err("OwnershipControls: Rules must hold exactly one rule".to_string());
    };
    let value = rule
        .get("ObjectOwnership")
        .and_then(Value::as_str)
        .ok_or("OwnershipControls: ObjectOwnership is required")?;
    if !crate::service::OBJECT_OWNERSHIP_VALUES.contains(&value) {
        return Err(format!(
            "OwnershipControls: invalid ObjectOwnership value: {value}"
        ));
    }
    Ok(Some(value.to_string()))
}

/// Apply the config properties of an `AWS::S3::Bucket` CloudFormation resource
/// to an existing `bucket` (a stack update), persisting each touched
/// subresource through `store` exactly as the corresponding `PutBucket*`
/// handler does, so a follow-up update that turns on versioning/encryption is
/// not a silent no-op.
///
/// Only properties actually present in `props` are applied; absent properties
/// leave the existing bucket state untouched (matching CFN update semantics for
/// the properties we model). Every property validates before anything is
/// written.
pub fn apply_cfn_bucket_properties(
    bucket: &mut S3Bucket,
    props: &Value,
    store: &Arc<dyn S3Store>,
) -> Result<(), String> {
    let staged = stage_cfn_bucket_properties(bucket, props)?;
    staged.persist(store, &bucket.name)?;
    if staged.meta_dirty {
        let meta = bucket_meta_snapshot(bucket);
        store
            .put_bucket_meta(&bucket.name, &meta)
            .map_err(|e| persist_err("meta", &bucket.name, e))?;
    }
    Ok(())
}

/// Validate every property, then apply them to `bucket` in memory and return
/// the subresource writes that make them durable.
fn stage_cfn_bucket_properties(
    bucket: &mut S3Bucket,
    props: &Value,
) -> Result<StagedWrites, String> {
    let mut staged = StagedWrites::default();
    let Some(obj) = props.as_object() else {
        return Ok(staged);
    };

    // OwnershipControls and AccessControl are resolved against each other and
    // against the Block Public Access the template ends up with, with the
    // refusals S3 itself gives -- but only for what the template changes. A
    // stack update re-sends unchanged properties, and an unchanged ownership
    // rule or ACL is not a new request to S3 that could be refused.
    let ownership = match obj.get("OwnershipControls") {
        Some(v) => cfn_object_ownership(v)?,
        None => None,
    };
    let canned = match obj.get("AccessControl") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            Some(cfn_canned_acl(s).ok_or_else(|| format!("AccessControl: invalid value: {s}"))?)
        }
        Some(_) => return Err("AccessControl: must be a string".to_string()),
    };
    let pab_xml = obj
        .get("PublicAccessBlockConfiguration")
        .and_then(build_public_access_block_xml);
    let current_ownership = bucket
        .ownership_controls
        .as_deref()
        .and_then(|xml| crate::service::extract_xml_value(xml, "ObjectOwnership"));
    let ownership_changed = ownership
        .as_ref()
        .is_some_and(|o| current_ownership.as_ref() != Some(o));
    let owner_id = bucket.acl_owner_id.clone();
    let acl_grants = canned
        .map(|acl| crate::service::canned_acl_grants(acl, &owner_id))
        .filter(|grants| *grants != bucket.acl_grants);
    if ownership_changed || acl_grants.is_some() {
        let final_ownership_enforced = match &ownership {
            Some(o) => o == "BucketOwnerEnforced",
            None => {
                crate::service::config::ownership_xml_enforced(bucket.ownership_controls.as_deref())
            }
        };
        let reaches_past_owner = match (canned, &acl_grants) {
            (Some(acl), Some(_)) => crate::service::acl_reaches_past_owner(acl, &owner_id),
            _ => !crate::service::config::grants_are_owner_only(&bucket.acl_grants, &owner_id),
        };
        if final_ownership_enforced && reaches_past_owner {
            return Err(
                "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting \
                 (Service: S3, Status Code: 400, Error Code: InvalidBucketAclWithObjectOwnership)"
                    .to_string(),
            );
        }
    }
    if let Some(grants) = &acl_grants {
        let effective_pab = pab_xml.as_deref().or(bucket.public_access_block.as_deref());
        let blocks = effective_pab.is_some_and(|x| {
            crate::service::config::PublicAccessBlockFlags::parse(x).block_public_acls
        });
        if blocks && crate::service::config::grants_are_public(grants) {
            return Err(
                "Bucket cannot have public ACLs set with BlockPublicAccess enabled \
                 (Service: S3, Status Code: 400, Error Code: \
                 InvalidBucketAclWithBlockPublicAccessError)"
                    .to_string(),
            );
        }
    }
    // Held to PutBucketEncryption's own validator, failing the resource with
    // the error that operation gives.
    let encryption_xml = obj.get("BucketEncryption").and_then(build_encryption_xml);
    if let Some(xml) = &encryption_xml {
        crate::service::config::validate_bucket_encryption_xml(xml)
            .map_err(|e| format!("{}: {}", e.code(), e.message()))?;
    }

    // Bound once so the string that was validated is the same one that gets
    // stored.
    // An absent `CorsConfiguration` is a no-op, and an empty `CorsRules: []`
    // expresses "no CORS" — but `CorsRules` present with the wrong shape (a
    // typo, or an intrinsic that resolved to an object or string) is a property
    // error. Skipping that silently deploys a green stack with no CORS applied
    // and nothing saying why, while every browser request then fails. Real
    // CloudFormation fails the resource on a type mismatch, so this does too.
    // An explicit `null` counts as absent, like every sibling property here —
    // only a `CorsConfiguration` that is actually an object is held to the
    // shape, and the message says which of the two problems it is.
    match obj.get("CorsConfiguration") {
        None | Some(Value::Null) => {}
        Some(Value::Object(cors)) => match cors.get("CorsRules") {
            Some(rules) if rules.as_array().is_none() => {
                return Err("CorsConfiguration: CorsRules must be a list of rules".to_string());
            }
            Some(rules) => {
                // Named here rather than left to the XML validator, which can
                // only answer `MalformedXML` and never says which property was
                // wrong. `as f64` also saturates, so a huge value would
                // otherwise render as `18446744073709551615`.
                for rule in rules.as_array().into_iter().flatten() {
                    let bad_max_age = rule.get("MaxAge").is_some_and(|v| match v {
                        Value::Number(_) => !v.as_f64().is_some_and(|n| {
                            n.fract() == 0.0 && (0.0..=f64::from(u32::MAX)).contains(&n)
                        }),
                        Value::String(s) => s.trim().parse::<u32>().is_err(),
                        _ => true,
                    });
                    if bad_max_age {
                        return Err(
                            "CorsConfiguration: MaxAge must be a whole number of seconds"
                                .to_string(),
                        );
                    }
                }
            }
            None => return Err("CorsConfiguration: CorsRules is required".to_string()),
        },
        Some(_) => {
            return Err("CorsConfiguration: must be an object with a CorsRules list".to_string());
        }
    }
    let cors_xml = obj.get("CorsConfiguration").and_then(build_cors_xml);
    if let Some(xml) = &cors_xml {
        crate::service::config::validate_cors_xml(xml)
            .map_err(|(code, message)| format!("{code}: {message}"))?;
    }
    // `versioning` and `eventbridge_enabled` live in the bucket meta snapshot,
    // so a single `put_bucket_meta` covers both, mirroring how the
    // versioning/notification handlers persist them.

    if let Some(v) = obj.get("VersioningConfiguration") {
        if let Some(status) = v.get("Status").and_then(Value::as_str) {
            if status == "Enabled" || status == "Suspended" {
                bucket.versioning = Some(status.to_string());
                staged.meta_dirty = true;
            }
        }
    }

    if let Some(xml) = encryption_xml {
        bucket.encryption_config = Some(xml.clone());
        staged.put(BucketSubresource::Encryption, xml);
    }

    if let Some(xml) = pab_xml {
        bucket.public_access_block = Some(xml.clone());
        staged.put(BucketSubresource::PublicAccessBlock, xml);
    }

    if let Some(grants) = acl_grants {
        // As CreateBucket does, an ACL the loader reconstructs on its own
        // (`private`, `aws-exec-read`) gets no `acl.toml`, and one that does
        // need it replaces whatever is there.
        if canned.is_some_and(crate::service::canned_acl_needs_sidecar) {
            let snap = AclSnapshot {
                owner_id: owner_id.clone(),
                grants: grants.iter().map(AclGrantSnapshot::from).collect(),
            };
            let payload = toml::to_string(&snap)
                .map_err(|e| format!("failed to serialize ACL for bucket {}: {e}", bucket.name))?;
            staged.put(BucketSubresource::Acl, payload);
        } else {
            staged.subresources.push((BucketSubresource::Acl, None));
        }
        bucket.acl_grants = grants;
    }

    if let Some(o) = ownership {
        let xml = crate::state::ownership_controls_xml(&o);
        bucket.ownership_controls = Some(xml.clone());
        staged.put(BucketSubresource::Ownership, xml);
    }

    if let Some(nc) = obj.get("NotificationConfiguration") {
        let (xml, eventbridge) = build_notification_xml(nc);
        bucket.notification_config = Some(xml.clone());
        bucket.eventbridge_enabled = eventbridge;
        staged.meta_dirty = true;
        staged.put(BucketSubresource::Notification, xml);
    }

    if let Some(tags) = obj.get("Tags") {
        if let Some(map) = build_tags(tags) {
            bucket.tags = map;
            let payload = toml::to_string(&TagsSnapshot {
                tags: bucket.tags.clone(),
            })
            .unwrap_or_default();
            staged.put(BucketSubresource::Tags, payload);
        }
    }

    if let Some(w) = obj.get("WebsiteConfiguration") {
        if let Some(xml) = build_website_xml(w) {
            bucket.website_config = Some(xml.clone());
            staged.put(BucketSubresource::Website, xml);
        }
    }

    // A `CorsConfiguration` that is present but expresses no rules clears the
    // bucket's CORS config. Leaving the previous one in place would keep the
    // bucket serving `Access-Control-Allow-Origin` after the template said it
    // should not. An absent property still leaves existing state untouched,
    // matching CFN update semantics for everything else here.
    // Only an explicitly empty `CorsRules: []` clears. `build_cors_xml` also
    // returns `None` when `CorsRules` is absent or not an array, and treating
    // those as "clear" would delete a live CORS config because an intrinsic
    // resolved to the wrong shape — every preflight then failing with nothing
    // in the stack output to say why.
    let clears_cors = obj
        .get("CorsConfiguration")
        .and_then(|c| c.get("CorsRules"))
        .and_then(Value::as_array)
        .is_some_and(|rules| rules.is_empty());
    if cors_xml.is_some() || clears_cors {
        match cors_xml {
            Some(xml) => {
                // The exact string validated above, before any subresource was
                // persisted.
                bucket.cors_config = Some(xml.clone());
                staged.put(BucketSubresource::Cors, xml);
            }
            None => {
                bucket.cors_config = None;
                staged.subresources.push((BucketSubresource::Cors, None));
            }
        }
    }

    if let Some(l) = obj.get("LifecycleConfiguration") {
        if let Some(xml) = build_lifecycle_xml(l) {
            bucket.lifecycle_config = Some(xml.clone());
            staged.put(BucketSubresource::Lifecycle, xml);
        }
    }

    if let Some(lg) = obj.get("LoggingConfiguration") {
        if let Some(xml) = build_logging_xml(lg) {
            bucket.logging_config = Some(xml.clone());
            staged.put(BucketSubresource::Logging, xml);
        }
    }

    Ok(staged)
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
        // `BlockedEncryptionTypes.EncryptionType` is a list (flattened in the
        // REST XML); a single string is accepted as a one-element list.
        let blocked: Vec<&str> = match rule
            .get("BlockedEncryptionTypes")
            .and_then(|b| b.get("EncryptionType"))
        {
            Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
            Some(Value::String(s)) => vec![s.as_str()],
            _ => Vec::new(),
        };
        if !blocked.is_empty() {
            body.push_str("<BlockedEncryptionTypes>");
            for t in blocked {
                body.push_str(&format!(
                    "<EncryptionType>{}</EncryptionType>",
                    xml_escape(t)
                ));
            }
            body.push_str("</BlockedEncryptionTypes>");
        }
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
    // CloudFormation sends the whole configuration in one PutPublicAccessBlock,
    // where an omitted flag is `false`. So every flag is written, and an empty
    // `PublicAccessBlockConfiguration: {}` turns all four off rather than
    // leaving the new bucket's all-on default in place.
    let mut body = String::new();
    for key in [
        "BlockPublicAcls",
        "IgnorePublicAcls",
        "BlockPublicPolicy",
        "RestrictPublicBuckets",
    ] {
        let b = obj.get(key).and_then(as_bool).unwrap_or(false);
        body.push_str(&format!("<{key}>{b}</{key}>"));
    }
    Some(wrap("PublicAccessBlockConfiguration", &body))
}

// ---- NotificationConfiguration ----

/// Build the notification-config XML and return whether EventBridge delivery is
/// enabled. Matches the tag shapes `parse_notification_config` reads on the
/// firing path (`<QueueConfiguration>/<Queue>`, `<TopicConfiguration>/<Topic>`,
/// `<CloudFunctionConfiguration>/<CloudFunction>`), so notifications actually
/// fire.
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
    // On the wire a Lambda configuration is `<CloudFunctionConfiguration>`
    // with the ARN in `<CloudFunction>` (the S3 model's XML names), which is
    // what GetBucketNotificationConfiguration clients parse.
    build_notification_entries(
        nc,
        "LambdaConfigurations",
        "CloudFunctionConfiguration",
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
        let wire_tag = if target_tag == "Function" {
            "CloudFunction"
        } else {
            target_tag
        };
        out.push_str(&format!("<{wire_tag}>{}</{wire_tag}>", xml_escape(arn)));
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
        // Rendered as an integer. `as_scalar` stringifies a JSON number
        // verbatim, so `MaxAge: 3600.0` would emit `3600.0` — which
        // `validate_cors_xml` rejects, failing the whole bucket resource and
        // taking unrelated versioning/encryption changes down with it.
        if let Some(max_age) = rule.get("MaxAge") {
            let seconds = max_age
                .as_f64()
                .filter(|n| n.fract() == 0.0 && *n >= 0.0)
                .map(|n| (n as u64).to_string())
                .or_else(|| as_scalar(max_age));
            if let Some(seconds) = seconds {
                body.push_str(&format!(
                    "<MaxAgeSeconds>{}</MaxAgeSeconds>",
                    xml_escape(&seconds)
                ));
            }
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

    /// A store that records the subresource writes and bucket removals it is
    /// asked for, and fails the write of one subresource kind -- the disk
    /// error a CloudFormation create must clean up after.
    #[derive(Default)]
    struct RecordingStore {
        fail_kind: Option<BucketSubresource>,
        calls: parking_lot::Mutex<Vec<String>>,
    }

    impl RecordingStore {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().clone()
        }
    }

    impl S3Store for RecordingStore {
        fn load(
            &self,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::S3StateSnapshot> {
            fakecloud_persistence::MemoryS3Store.load()
        }
        fn put_bucket_meta(
            &self,
            bucket: &str,
            _meta: &fakecloud_persistence::BucketMeta,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.calls.lock().push(format!("meta {bucket}"));
            Ok(())
        }
        fn put_bucket_subresource(
            &self,
            bucket: &str,
            kind: BucketSubresource,
            _payload: &str,
        ) -> fakecloud_persistence::StoreResult<()> {
            if self.fail_kind == Some(kind) {
                return Err(StoreError::Other("disk full".to_string()));
            }
            self.calls.lock().push(format!("put {bucket} {kind:?}"));
            Ok(())
        }
        fn delete_bucket_subresource(
            &self,
            bucket: &str,
            kind: BucketSubresource,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.calls.lock().push(format!("delete {bucket} {kind:?}"));
            Ok(())
        }
        fn delete_bucket(&self, bucket: &str) -> fakecloud_persistence::StoreResult<()> {
            self.calls.lock().push(format!("delete-bucket {bucket}"));
            Ok(())
        }
        fn put_object(
            &self,
            _bucket: &str,
            _key: &str,
            _version: Option<&str>,
            _body: fakecloud_persistence::BodySource,
            _meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            Err(StoreError::NotSupported)
        }
        fn replace_object_body(
            &self,
            _bucket: &str,
            _key: &str,
            _version: Option<&str>,
            _body: fakecloud_persistence::BodySource,
            _meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            Err(StoreError::NotSupported)
        }
        fn put_object_meta(
            &self,
            _bucket: &str,
            _key: &str,
            _version: Option<&str>,
            _meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<()> {
            Err(StoreError::NotSupported)
        }
        fn delete_object(
            &self,
            _bucket: &str,
            _key: &str,
            _version: Option<&str>,
        ) -> fakecloud_persistence::StoreResult<()> {
            Err(StoreError::NotSupported)
        }
        fn open_object_body(
            &self,
            _body: &fakecloud_persistence::BodyRef,
        ) -> fakecloud_persistence::StoreResult<bytes::Bytes> {
            Err(StoreError::NotSupported)
        }
        fn mpu_create(
            &self,
            _bucket: &str,
            _upload_id: &str,
            _init: &fakecloud_persistence::MpuInit,
        ) -> fakecloud_persistence::StoreResult<()> {
            Err(StoreError::NotSupported)
        }
        fn mpu_put_part(
            &self,
            _bucket: &str,
            _upload_id: &str,
            _part_number: u32,
            _body: fakecloud_persistence::BodySource,
            _etag: &str,
            _checksum: Option<fakecloud_persistence::PartChecksum<'_>>,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            Err(StoreError::NotSupported)
        }
        fn mpu_abort(
            &self,
            _bucket: &str,
            _upload_id: &str,
        ) -> fakecloud_persistence::StoreResult<()> {
            Err(StoreError::NotSupported)
        }
        fn mpu_complete(
            &self,
            _bucket: &str,
            _upload_id: &str,
            _final_key: &str,
            _version: Option<&str>,
            _meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            Err(StoreError::NotSupported)
        }
    }

    #[test]
    fn cfn_create_writes_meta_first_and_cleans_up_a_failed_write() {
        let rec = Arc::new(RecordingStore::default());
        let store: Arc<dyn S3Store> = rec.clone();
        let mut b = bucket();
        apply_cfn_new_bucket(
            &mut b,
            &json!({ "Tags": [{ "Key": "k", "Value": "v" }] }),
            &store,
        )
        .unwrap();
        let calls = rec.calls();
        assert_eq!(
            calls.first().map(String::as_str),
            Some("meta b"),
            "{calls:?}"
        );
        for kind in ["Ownership", "PublicAccessBlock", "Encryption", "Tags"] {
            assert!(
                calls.contains(&format!("put b {kind}")),
                "{kind}: {calls:?}"
            );
        }
        assert!(
            !calls.iter().any(|c| c.starts_with("delete-bucket")),
            "{calls:?}"
        );

        // The Tags write fails after meta and the create-time settings were
        // written: the whole bucket comes back off the store.
        let rec = Arc::new(RecordingStore {
            fail_kind: Some(BucketSubresource::Tags),
            ..Default::default()
        });
        let store: Arc<dyn S3Store> = rec.clone();
        let mut b = bucket();
        let err = apply_cfn_new_bucket(
            &mut b,
            &json!({ "Tags": [{ "Key": "k", "Value": "v" }] }),
            &store,
        )
        .unwrap_err();
        assert!(err.contains("disk full"), "{err}");
        let calls = rec.calls();
        assert_eq!(
            calls.first().map(String::as_str),
            Some("meta b"),
            "{calls:?}"
        );
        assert_eq!(
            calls.last().map(String::as_str),
            Some("delete-bucket b"),
            "{calls:?}"
        );

        // A refused property writes nothing at all.
        let rec = Arc::new(RecordingStore::default());
        let store: Arc<dyn S3Store> = rec.clone();
        let mut b = bucket();
        apply_cfn_new_bucket(
            &mut b,
            &json!({ "AccessControl": "LogDeliveryWrite" }),
            &store,
        )
        .unwrap_err();
        assert!(rec.calls().is_empty(), "{:?}", rec.calls());
    }

    #[test]
    fn cfn_owner_only_acls_get_no_acl_sidecar() {
        for (acl, sidecar) in [
            ("Private", false),
            ("AwsExecRead", false),
            ("LogDeliveryWrite", true),
        ] {
            let rec = Arc::new(RecordingStore::default());
            let store: Arc<dyn S3Store> = rec.clone();
            let mut b = bucket();
            apply_cfn_new_bucket(
                &mut b,
                &json!({
                    "AccessControl": acl,
                    "OwnershipControls": { "Rules": [{ "ObjectOwnership": "ObjectWriter" }] }
                }),
                &store,
            )
            .unwrap();
            assert_eq!(
                rec.calls().contains(&"put b Acl".to_string()),
                sidecar,
                "{acl}: {:?}",
                rec.calls()
            );
        }
    }

    #[test]
    fn cfn_update_checks_ownership_only_for_what_the_template_changes() {
        // A bucket whose ownership is BucketOwnerEnforced while its stored ACL
        // still grants others (a state an older build could persist).
        let mut b = bucket();
        b.apply_new_bucket_defaults();
        b.acl_grants.extend(crate::service::canned_acl_grants(
            "log-delivery-write",
            "123456789012",
        ));
        let unchanged = json!({
            "OwnershipControls": { "Rules": [{ "ObjectOwnership": "BucketOwnerEnforced" }] }
        });
        apply_cfn_bucket_properties(&mut b, &unchanged, &store()).unwrap();
        apply_cfn_bucket_properties(&mut b, &json!({}), &store()).unwrap();

        // Changing to BucketOwnerEnforced over such an ACL is refused.
        let mut b = bucket();
        b.apply_new_bucket_defaults();
        b.ownership_controls = Some(crate::state::ownership_controls_xml("ObjectWriter"));
        b.acl_grants.extend(crate::service::canned_acl_grants(
            "log-delivery-write",
            "123456789012",
        ));
        let err = apply_cfn_bucket_properties(&mut b, &unchanged, &store()).unwrap_err();
        assert!(err.contains("InvalidBucketAclWithObjectOwnership"), "{err}");
    }

    #[test]
    fn cfn_empty_public_access_block_turns_every_flag_off() {
        for pab in [json!({}), json!({ "Unknown": true })] {
            let mut b = bucket();
            apply_cfn_new_bucket(
                &mut b,
                &json!({ "PublicAccessBlockConfiguration": pab }),
                &store(),
            )
            .unwrap();
            let xml = b.public_access_block.unwrap();
            for flag in [
                "BlockPublicAcls",
                "IgnorePublicAcls",
                "BlockPublicPolicy",
                "RestrictPublicBuckets",
            ] {
                assert!(
                    xml.contains(&format!("<{flag}>false</{flag}>")),
                    "{flag}: {xml}"
                );
            }
        }
    }

    #[test]
    fn cfn_encryption_is_held_to_put_bucket_encryption_validation() {
        let mut b = bucket();
        let err = apply_cfn_new_bucket(
            &mut b,
            &json!({
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{
                        "ServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" },
                        "BlockedEncryptionTypes": { "EncryptionType": ["SSE-KMS"] }
                    }]
                }
            }),
            &store(),
        )
        .unwrap_err();
        assert!(err.starts_with("MalformedXML:"), "{err}");
    }

    #[test]
    fn new_cfn_bucket_gets_the_create_bucket_defaults() {
        let mut b = bucket();
        apply_cfn_new_bucket(&mut b, &json!({}), &store()).unwrap();
        assert!(crate::service::config::ownership_xml_enforced(
            b.ownership_controls.as_deref()
        ));
        assert_eq!(
            b.public_access_block.as_deref(),
            Some(crate::state::DEFAULT_PUBLIC_ACCESS_BLOCK_XML)
        );
        assert_eq!(
            b.encryption_config.as_deref(),
            Some(crate::state::DEFAULT_BUCKET_ENCRYPTION_XML)
        );
    }

    #[test]
    fn cfn_ownership_controls_and_access_control_follow_the_api_rules() {
        // AccessControl under the default ownership is refused, as CreateBucket
        // refuses an ACL with BucketOwnerEnforced.
        let mut b = bucket();
        let err = apply_cfn_new_bucket(
            &mut b,
            &json!({ "AccessControl": "LogDeliveryWrite" }),
            &store(),
        )
        .unwrap_err();
        assert!(err.contains("InvalidBucketAclWithObjectOwnership"), "{err}");

        // With ACLs enabled the ACL applies.
        let mut b = bucket();
        apply_cfn_new_bucket(
            &mut b,
            &json!({
                "AccessControl": "LogDeliveryWrite",
                "OwnershipControls": { "Rules": [{ "ObjectOwnership": "ObjectWriter" }] }
            }),
            &store(),
        )
        .unwrap();
        assert!(b
            .ownership_controls
            .as_deref()
            .unwrap()
            .contains("<ObjectOwnership>ObjectWriter</ObjectOwnership>"));
        assert!(b.acl_grants.iter().any(|g| g
            .grantee_uri
            .as_deref()
            .is_some_and(|u| u.contains("LogDelivery"))));

        // A public ACL needs the template to lift Block Public Access.
        let mut b = bucket();
        let err = apply_cfn_new_bucket(
            &mut b,
            &json!({
                "AccessControl": "PublicRead",
                "OwnershipControls": { "Rules": [{ "ObjectOwnership": "ObjectWriter" }] }
            }),
            &store(),
        )
        .unwrap_err();
        assert!(
            err.contains("InvalidBucketAclWithBlockPublicAccessError"),
            "{err}"
        );
        let mut b = bucket();
        apply_cfn_new_bucket(
            &mut b,
            &json!({
                "AccessControl": "PublicRead",
                "OwnershipControls": { "Rules": [{ "ObjectOwnership": "ObjectWriter" }] },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": false, "IgnorePublicAcls": false,
                    "BlockPublicPolicy": false, "RestrictPublicBuckets": false
                }
            }),
            &store(),
        )
        .unwrap();
        assert!(crate::service::config::grants_are_public(&b.acl_grants));
    }

    #[test]
    fn bucket_policy_blocked_follows_block_public_policy() {
        let public = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        let scoped = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"cloudtrail.amazonaws.com"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        let mut b = bucket();
        apply_cfn_new_bucket(&mut b, &json!({}), &store()).unwrap();
        assert!(bucket_policy_blocked(&b, public));
        assert!(!bucket_policy_blocked(&b, scoped));
        let mut open = bucket();
        apply_cfn_new_bucket(
            &mut open,
            &json!({ "PublicAccessBlockConfiguration": { "BlockPublicPolicy": false } }),
            &store(),
        )
        .unwrap();
        assert!(!bucket_policy_blocked(&open, public));
    }

    #[test]
    fn cfn_encryption_carries_blocked_encryption_types() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{
                        "ServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" },
                        "BlockedEncryptionTypes": { "EncryptionType": ["NONE"] }
                    }]
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.encryption_config.unwrap();
        assert!(xml.contains("<BlockedEncryptionTypes><EncryptionType>NONE</EncryptionType></BlockedEncryptionTypes>"), "{xml}");
        assert!(!crate::state::encryption_blocks_sse_c(&xml));
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

    /// `AWS::S3::Bucket` takes an aws:kms default with no KMSMasterKeyID,
    /// as AWS does (objects then use the `aws/s3` key); it is stored
    /// without a key.
    #[test]
    fn encryption_kms_rule_without_a_key() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{
                        "ServerSideEncryptionByDefault": { "SSEAlgorithm": "aws:kms" }
                    }]
                }
            }),
            &store(),
        )
        .unwrap();
        let xml = b.encryption_config.unwrap();
        assert!(xml.contains("<SSEAlgorithm>aws:kms</SSEAlgorithm>"));
        assert!(!xml.contains("KMSMasterKeyID"));
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
        // An omitted flag is false, as in the PutPublicAccessBlock CFN sends.
        assert!(xml.contains("<BlockPublicPolicy>false</BlockPublicPolicy>"));
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
        // Lambda uses the wire shape clients parse:
        // <CloudFunctionConfiguration>/<CloudFunction>.
        assert!(xml.contains("<CloudFunctionConfiguration>"));
        assert!(xml.contains(
            "<CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:f</CloudFunction>"
        ));
        // ... and the firing path still sees the target.
        let targets = crate::service::notifications::parse_notification_config(&xml);
        assert!(targets
            .iter()
            .any(|t| t.arn == "arn:aws:lambda:us-east-1:123456789012:function:f"));
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
    fn cors_rules_with_the_wrong_shape_fails_the_resource() {
        // Skipping silently would deploy a green stack with no CORS applied and
        // nothing saying why, while every browser request against it fails.
        // An explicit null is absent, not a shape error — every sibling
        // property tolerates it, and failing the stack over one would be a
        // regression from "deploys with no CORS".
        let mut b = bucket();
        apply_cfn_bucket_properties(&mut b, &json!({"CorsConfiguration": null}), &store())
            .expect("an explicit null is treated as absent");
        assert!(b.cors_config.is_none());

        for bad in [
            json!({"CorsRules": {"AllowedMethods": ["GET"]}}),
            json!({}),
            json!("not-an-object"),
        ] {
            let mut b = bucket();
            let err =
                apply_cfn_bucket_properties(&mut b, &json!({ "CorsConfiguration": bad }), &store())
                    .expect_err("a CorsRules type mismatch is a property error");
            assert!(err.contains("Cors"), "{err}");
            assert!(b.cors_config.is_none());
        }
    }

    #[test]
    fn cors_max_age_out_of_range_names_the_property() {
        // `as u64` saturates, so this would otherwise render as
        // 18446744073709551615 and fail with a generic MalformedXML that names
        // nothing the operator can act on.
        let mut b = bucket();
        let err = apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "CorsConfiguration": {
                    "CorsRules": [{
                        "AllowedMethods": ["GET"],
                        "AllowedOrigins": ["*"],
                        "MaxAge": 1e19
                    }]
                }
            }),
            &store(),
        )
        .expect_err("an out-of-range max-age is a property error");
        assert!(err.contains("MaxAge"), "{err}");
    }

    #[test]
    fn cors_max_age_renders_an_integral_float_as_an_integer() {
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "CorsConfiguration": {
                    "CorsRules": [{
                        "AllowedMethods": ["GET"],
                        "AllowedOrigins": ["*"],
                        "MaxAge": 3600.0
                    }]
                }
            }),
            &store(),
        )
        .expect("an integral float is a valid max-age");
        // `3600.0` would fail validation and take the whole bucket resource
        // down with it, including unrelated properties.
        assert!(b
            .cors_config
            .unwrap()
            .contains("<MaxAgeSeconds>3600</MaxAgeSeconds>"));
    }

    #[test]
    fn cors_invalid_rule_is_rejected_before_anything_is_persisted() {
        let mut b = bucket();
        let err = apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "VersioningConfiguration": {"Status": "Enabled"},
                // No AllowedMethods: matches nothing at request time.
                "CorsConfiguration": {"CorsRules": [{"AllowedOrigins": ["*"]}]}
            }),
            &store(),
        )
        .expect_err("a rule that can never match must not deploy");
        assert!(err.contains("MalformedXML"), "{err}");
        // Validation runs before any property is applied, so nothing is left
        // half-written.
        assert!(b.cors_config.is_none());
        assert!(b.versioning.is_none());
    }

    #[test]
    fn empty_cors_rules_clears_an_existing_config() {
        let store = store();
        let mut b = bucket();
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "CorsConfiguration": {
                    "CorsRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]
                }
            }),
            &store,
        )
        .unwrap();
        assert!(b.cors_config.is_some());

        // An explicitly empty rule list means "no CORS", so the live config has
        // to go rather than linger and keep serving allow-origin.
        apply_cfn_bucket_properties(
            &mut b,
            &json!({"CorsConfiguration": {"CorsRules": []}}),
            &store,
        )
        .unwrap();
        assert!(b.cors_config.is_none());

        // An absent property leaves existing state untouched, as for every
        // other property here.
        apply_cfn_bucket_properties(
            &mut b,
            &json!({
                "CorsConfiguration": {
                    "CorsRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]
                }
            }),
            &store,
        )
        .unwrap();
        apply_cfn_bucket_properties(&mut b, &json!({"Tags": []}), &store).unwrap();
        assert!(b.cors_config.is_some());
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
