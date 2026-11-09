//! Whether a stack update replaces a resource, and whether the replacement can
//! happen at all.
//!
//! Decided from the CloudFormation registry's schema facts, vendored as
//! `data/resource_schema_facts.json.gz` by `scripts/gen-cfn-resource-schemas.py`:
//! a changed create-only property means a new physical resource, as does a
//! conditional create-only one on the allowlist below. A type the registry does
//! not list has no such property.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::Value;

const DATA_GZ: &[u8] = include_bytes!("../data/resource_schema_facts.json.gz");

/// The conditional create-only paths known to replace the resource. The registry
/// says only that a replacement may follow from changing such a path, under a
/// condition it does not state: of its 172 paths on 49 types, many are ordinary
/// in-place updates (`AWS::EC2::Instance`'s `InstanceType`, say). So a conditional
/// path updates in place unless listed here, since the two ways of being wrong
/// are not equal: a false replacement breaks a deploy with the custom-name
/// failure or new physical ids, while a false in-place only keeps a physical id.
/// Add a path only with a recording, or with AWS's documented "Update requires:
/// Replacement" for that condition.
const CONDITIONAL_REPLACEMENTS: &[(&str, &[&str])] = &[
    // Recorded on a real account, 2026-11-09.
    ("AWS::DynamoDB::Table", &["KeySchema"]),
    ("AWS::Events::Rule", &["EventBusName"]),
];

/// Properties beside a resource's name that decide its physical identity, which
/// the registry's primary identifier does not show. A rule's id includes its bus
/// (recorded: moving a named rule to another bus replaces it fine, since the new
/// rule is a different resource), and an alias lives under its function (inferred
/// from the API, which keys an alias by both).
const IDENTITY_SCOPES: &[(&str, &[&str])] = &[
    ("AWS::Events::Rule", &["EventBusName"]),
    ("AWS::Lambda::Alias", &["FunctionName"]),
];

fn conditional_replaces(resource_type: &str, path: &str) -> bool {
    CONDITIONAL_REPLACEMENTS
        .iter()
        .any(|(listed, paths)| *listed == resource_type && paths.contains(&path))
}

#[derive(Debug, Default, Deserialize)]
struct TypeFacts {
    /// `createOnlyProperties`.
    #[serde(default, rename = "c")]
    create_only: Vec<String>,
    /// `conditionalCreateOnlyProperties`.
    #[serde(default, rename = "o")]
    conditional_create_only: Vec<String>,
    /// `primaryIdentifier` members that are not read-only.
    #[serde(default, rename = "p")]
    writable_primary: Vec<String>,
    /// Whether a `primaryIdentifier` member is read-only (the service assigns
    /// the identifier, and the writable members are references to a parent).
    #[serde(default, rename = "r")]
    read_only_primary: u8,
}

fn facts() -> &'static HashMap<String, TypeFacts> {
    static FACTS: OnceLock<HashMap<String, TypeFacts>> = OnceLock::new();
    FACTS.get_or_init(|| {
        fakecloud_core::embedded::decode_gz_json(DATA_GZ, "CloudFormation resource schema facts")
    })
}

/// What an update from `old` to `new` properties does to the physical resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Classification {
    /// The existing physical resource is updated.
    InPlace,
    /// A new physical resource is needed. `changed` names the property paths
    /// responsible; `conditional` is set when only allowlisted conditional
    /// create-only properties changed, which a change set reports as `Conditional`.
    Replacement {
        changed: Vec<String>,
        conditional: bool,
    },
}

impl Classification {
    pub(crate) fn is_replacement(&self) -> bool {
        matches!(self, Self::Replacement { .. })
    }
}

/// Classify an update of a resource of `resource_type` from `old` to `new`
/// resolved properties.
pub(crate) fn classify(resource_type: &str, old: &Value, new: &Value) -> Classification {
    let Some(facts) = facts().get(resource_type) else {
        return Classification::InPlace;
    };
    let changed_paths = |paths: &[String]| -> Vec<String> {
        paths
            .iter()
            .filter(|path| path_changed(path, old, new))
            .cloned()
            .collect()
    };
    let hard = changed_paths(&facts.create_only);
    let mut conditional = changed_paths(&facts.conditional_create_only);
    conditional.retain(|path| conditional_replaces(resource_type, path));
    if hard.is_empty() && conditional.is_empty() {
        return Classification::InPlace;
    }
    let only_conditional = hard.is_empty();
    let mut changed = hard;
    changed.extend(conditional);
    Classification::Replacement {
        changed,
        conditional: only_conditional,
    }
}

/// The create-only property paths a change of `old` to `new` touches, however
/// they are classified: what Cloud Control refuses to patch.
pub(crate) fn changed_create_only_paths(
    resource_type: &str,
    old: &Value,
    new: &Value,
) -> Vec<String> {
    match classify(resource_type, old, new) {
        Classification::InPlace => Vec::new(),
        Classification::Replacement { changed, .. } => changed,
    }
}

/// The top-level properties that make up a resource's own name. The primary
/// identifier when the caller sets all of it, plus a create-only `Name` or
/// `<Type>Name`. A read-only primary member means the service assigns the
/// identifier, so a writable member beside it is a reference to a parent (the
/// `FunctionName` of an `AWS::Lambda::Permission`), not this resource's name.
fn name_properties(resource_type: &str) -> Vec<&'static str> {
    let Some(facts) = facts().get(resource_type) else {
        return Vec::new();
    };
    let type_name = resource_type.rsplit("::").next().unwrap_or(resource_type);
    let is_own_name = |property: &str| {
        property == "Name"
            || property
                .strip_suffix("Name")
                .is_some_and(|stem| !stem.is_empty() && type_name.ends_with(stem))
    };
    let mut names: Vec<&'static str> = Vec::new();
    if facts.read_only_primary == 0 {
        names.extend(facts.writable_primary.iter().map(String::as_str));
    }
    for property in facts
        .create_only
        .iter()
        .chain(&facts.conditional_create_only)
    {
        if is_own_name(property) && !names.contains(&property.as_str()) {
            names.push(property);
        }
    }
    names.retain(|name| !name.contains('/'));
    names
}

/// What, beside the name, makes up the identity: the listed scopes, plus the
/// writable members of a primary identifier the service completes with a
/// read-only one (an ECS service's `Cluster`, a web ACL's `Scope`).
fn identity_scopes(resource_type: &str) -> Vec<&'static str> {
    let mut scopes: Vec<&'static str> = IDENTITY_SCOPES
        .iter()
        .filter(|(listed, _)| *listed == resource_type)
        .flat_map(|(_, scopes)| scopes.iter().copied())
        .collect();
    if let Some(facts) = facts().get(resource_type) {
        if facts.read_only_primary != 0 {
            scopes.extend(facts.writable_primary.iter().map(String::as_str));
        }
    }
    scopes.retain(|scope| !scope.contains('/'));
    scopes
}

/// Whether two resources of this type can hold one name without colliding.
pub(crate) fn name_is_scoped(resource_type: &str) -> bool {
    !identity_scopes(resource_type).is_empty()
}

/// The physical identity a template gives this resource explicitly, `None` when
/// any part of the name is left for CloudFormation to generate. The parts are
/// joined so the identity compares as one value; a scope may be absent, as a
/// rule on the default bus leaves its bus out.
pub(crate) fn custom_name(resource_type: &str, props: &Value) -> Option<String> {
    let names = name_properties(resource_type);
    if names.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(names.len());
    for name in names {
        parts.push(scalar_text(props.get(name)?).filter(|text| !text.is_empty())?);
    }
    for scope in identity_scopes(resource_type) {
        parts.push(props.get(scope).and_then(scalar_text).unwrap_or_default());
    }
    Some(parts.join("|"))
}

/// The name to print in a failure reason: the resource's own `*Name` when the
/// name has several parts.
pub(crate) fn display_name(resource_type: &str, props: &Value) -> Option<String> {
    let names = name_properties(resource_type);
    let chosen = names
        .iter()
        .find(|name| name.ends_with("Name"))
        .or_else(|| names.first())?;
    scalar_text(props.get(*chosen)?)
}

/// The E1 rule: a replacement that would produce the same physical identity
/// cannot happen, since the old resource still holds it. Returns the name to ask
/// the caller to change.
pub(crate) fn custom_name_blocking_replacement(
    resource_type: &str,
    old: &Value,
    new: &Value,
) -> Option<String> {
    let new_name = custom_name(resource_type, new)?;
    let old_name = custom_name(resource_type, old)?;
    (new_name == old_name)
        .then(|| display_name(resource_type, new))
        .flatten()
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// The values a schema path selects. Paths are `/`-separated; `*` matches every
/// member of a collection, and a collection met on the way is walked through
/// whether or not the path spells the `*` out.
fn values_at<'a>(value: &'a Value, segments: &[&str], out: &mut Vec<&'a Value>) {
    if value.is_null() {
        return;
    }
    let Some((segment, rest)) = segments.split_first() else {
        out.push(value);
        return;
    };
    match value {
        Value::Array(items) => {
            let inner = if *segment == "*" { rest } else { segments };
            for item in items {
                values_at(item, inner, out);
            }
        }
        Value::Object(members) => {
            if *segment == "*" {
                for member in members.values() {
                    values_at(member, rest, out);
                }
            } else if let Some(member) = members.get(*segment) {
                values_at(member, rest, out);
            }
        }
        _ => {}
    }
}

fn path_changed(path: &str, old: &Value, new: &Value) -> bool {
    let segments: Vec<&str> = path.split('/').collect();
    let (mut before, mut after) = (Vec::new(), Vec::new());
    values_at(old, &segments, &mut before);
    values_at(new, &segments, &mut after);
    before.len() != after.len() || before.iter().zip(&after).any(|(a, b)| !same_value(a, b))
}

/// Template values compared the way CloudFormation sees them: a parameter
/// arrives as a string, so `"5"` and `5` (or `"true"` and `true`) are the same
/// value, and a member set to null is an absent member.
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_value(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            let set = |m: &'_ serde_json::Map<String, Value>| {
                m.iter().filter(|(_, v)| !v.is_null()).count()
            };
            set(a) == set(b)
                && a.iter()
                    .filter(|(_, v)| !v.is_null())
                    .all(|(k, v)| b.get(k).is_some_and(|other| same_value(v, other)))
        }
        _ => match (scalar_text(a), scalar_text(b)) {
            (Some(a), Some(b)) => a == b,
            _ => a == b,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_registry_type_is_vendored() {
        // 2107 public AWS types at the time of the dump; a much smaller number
        // means a truncated regeneration.
        assert!(facts().len() > 2000, "{}", facts().len());
        for resource_type in [
            "AWS::DynamoDB::Table",
            "AWS::SQS::Queue",
            "AWS::KinesisFirehose::DeliveryStream",
        ] {
            assert!(facts().contains_key(resource_type), "{resource_type}");
        }
    }

    #[test]
    fn unknown_types_never_replace() {
        assert_eq!(
            classify("Custom::Thing", &json!({"A": 1}), &json!({"A": 2})),
            Classification::InPlace
        );
    }

    #[test]
    fn changed_create_only_property_replaces_and_others_do_not() {
        let old = json!({"QueueName": "a", "VisibilityTimeout": 30});
        assert_eq!(
            classify(
                "AWS::SQS::Queue",
                &old,
                &json!({"QueueName": "a", "VisibilityTimeout": 60})
            ),
            Classification::InPlace
        );
        assert_eq!(
            classify(
                "AWS::SQS::Queue",
                &old,
                &json!({"QueueName": "b", "VisibilityTimeout": 30})
            ),
            Classification::Replacement {
                changed: vec!["QueueName".to_string()],
                conditional: false
            }
        );
    }

    #[test]
    fn adding_or_removing_a_create_only_property_replaces() {
        for (old, new) in [
            (json!({}), json!({"QueueName": "a"})),
            (json!({"QueueName": "a"}), json!({})),
        ] {
            assert!(classify("AWS::SQS::Queue", &old, &new).is_replacement());
        }
    }

    #[test]
    fn parameter_strings_compare_equal_to_their_typed_values() {
        assert!(!classify(
            "AWS::SQS::Queue",
            &json!({"FifoQueue": "true"}),
            &json!({"FifoQueue": true})
        )
        .is_replacement());
        assert!(
            !classify("AWS::SQS::Queue", &json!({"FifoQueue": null}), &json!({})).is_replacement()
        );
    }

    #[test]
    fn firehose_source_and_name_replace_but_destinations_do_not() {
        let stream = "AWS::KinesisFirehose::DeliveryStream";
        let base = json!({
            "DeliveryStreamName": "s",
            "ExtendedS3DestinationConfiguration": {"BucketARN": "a"}
        });
        let mut other_bucket = base.clone();
        other_bucket["ExtendedS3DestinationConfiguration"]["BucketARN"] = json!("b");
        assert!(!classify(stream, &base, &other_bucket).is_replacement());
        let mut sourced = base.clone();
        sourced["KinesisStreamSourceConfiguration"] = json!({"KinesisStreamARN": "k"});
        assert!(classify(stream, &base, &sourced).is_replacement());
        let mut typed = base.clone();
        typed["DeliveryStreamType"] = json!("KinesisStreamAsSource");
        assert!(classify(stream, &base, &typed).is_replacement());
    }

    #[test]
    fn nested_create_only_paths_only_trigger_on_their_own_member() {
        let stream = "AWS::KinesisFirehose::DeliveryStream";
        let with = |subnets: Value, index: &str| {
            json!({"ElasticsearchDestinationConfiguration": {
                "IndexName": index,
                "VpcConfiguration": {"SubnetIds": subnets}
            }})
        };
        assert!(
            !classify(stream, &with(json!(["s1"]), "a"), &with(json!(["s1"]), "b"))
                .is_replacement()
        );
        assert!(
            classify(stream, &with(json!(["s1"]), "a"), &with(json!(["s2"]), "a")).is_replacement()
        );
    }

    #[test]
    fn wildcards_and_collections_walk_every_member() {
        let segments = ["Items", "*", "Key"];
        let value = json!({"Items": [{"Key": 1}, {"Key": 2}, {"Other": 3}]});
        let mut out = Vec::new();
        values_at(&value, &segments, &mut out);
        assert_eq!(out, vec![&json!(1), &json!(2)]);
        // A collection the path does not spell out is walked the same way.
        let mut out = Vec::new();
        values_at(&value, &["Items", "Key"], &mut out);
        assert_eq!(out, vec![&json!(1), &json!(2)]);
        // `*` over a map.
        let map = json!({"M": {"a": {"K": 1}}});
        let mut out = Vec::new();
        values_at(&map, &["M", "*", "K"], &mut out);
        assert_eq!(out, vec![&json!(1)]);
    }

    #[test]
    fn custom_named_replacement_is_detected_per_type() {
        let table = "AWS::DynamoDB::Table";
        assert_eq!(
            custom_name_blocking_replacement(
                table,
                &json!({"TableName": "t"}),
                &json!({"TableName": "t", "KeySchema": [{"AttributeName": "b"}]})
            )
            .as_deref(),
            Some("t")
        );
        assert_eq!(
            custom_name_blocking_replacement(
                table,
                &json!({"TableName": "t"}),
                &json!({"TableName": "u"})
            ),
            None
        );
        assert_eq!(
            custom_name_blocking_replacement(table, &json!({}), &json!({})),
            None
        );

        // SQS's primary identifier is the read-only QueueUrl, so its name comes
        // from the create-only QueueName.
        assert_eq!(
            custom_name_blocking_replacement(
                "AWS::SQS::Queue",
                &json!({"QueueName": "q.fifo"}),
                &json!({"QueueName": "q.fifo", "FifoQueue": true})
            )
            .as_deref(),
            Some("q.fifo")
        );
        assert_eq!(
            custom_name_blocking_replacement(
                "AWS::KinesisFirehose::DeliveryStream",
                &json!({"DeliveryStreamName": "s"}),
                &json!({"DeliveryStreamName": "s", "DeliveryStreamType": "KinesisStreamAsSource"})
            )
            .as_deref(),
            Some("s")
        );
    }

    #[test]
    fn references_to_a_parent_are_not_a_custom_name() {
        // A permission's or a version's FunctionName names the function, not
        // the resource, so changing another create-only property replaces.
        for (resource_type, old, new) in [
            (
                "AWS::Lambda::Permission",
                json!({"FunctionName": "f", "Principal": "a"}),
                json!({"FunctionName": "f", "Principal": "b"}),
            ),
            (
                "AWS::Lambda::Version",
                json!({"FunctionName": "f", "Description": "a"}),
                json!({"FunctionName": "f", "Description": "b"}),
            ),
        ] {
            assert!(
                classify(resource_type, &old, &new).is_replacement(),
                "{resource_type}"
            );
            assert_eq!(
                custom_name_blocking_replacement(resource_type, &old, &new),
                None,
                "{resource_type}"
            );
        }
        // An ECS service's Cluster is a writable primary member beside the
        // read-only ServiceArn, and ServiceName is what names it.
        assert_eq!(
            custom_name_blocking_replacement(
                "AWS::ECS::Service",
                &json!({"Cluster": "c", "ServiceName": "a"}),
                &json!({"Cluster": "c", "ServiceName": "b"})
            ),
            None
        );
    }

    #[test]
    fn conditional_paths_update_in_place_unless_allowlisted() {
        // Recorded: changing a table's key schema replaces it.
        assert_eq!(
            classify(
                "AWS::DynamoDB::Table",
                &json!({"KeySchema": [{"AttributeName": "a"}]}),
                &json!({"KeySchema": [{"AttributeName": "b"}]})
            ),
            Classification::Replacement {
                changed: vec!["KeySchema".to_string()],
                conditional: true
            }
        );
        assert!(classify(
            "AWS::Events::Rule",
            &json!({"EventBusName": "a"}),
            &json!({"EventBusName": "b"})
        )
        .is_replacement());
        // Vendored, but not known to replace.
        assert!(!facts()["AWS::EC2::Instance"]
            .conditional_create_only
            .is_empty());
        // Recorded: each of these kept its physical id.
        for (resource_type, property) in [
            ("AWS::EC2::Instance", "InstanceType"),
            ("AWS::EC2::Instance", "UserData"),
            ("AWS::RDS::DBInstance", "BackupRetentionPeriod"),
            ("AWS::AutoScaling::AutoScalingGroup", "LaunchTemplate"),
            ("AWS::AutoScaling::AutoScalingGroup", "VPCZoneIdentifier"),
        ] {
            assert_eq!(
                classify(
                    resource_type,
                    &json!({property: "a"}),
                    &json!({property: "b"})
                ),
                Classification::InPlace,
                "{resource_type}.{property}"
            );
        }
        // Plain create-only on the same type still replaces.
        assert!(classify(
            "AWS::EC2::Instance",
            &json!({"ImageId": "a"}),
            &json!({"ImageId": "b"})
        )
        .is_replacement());
    }

    #[test]
    fn a_replacement_blocks_only_on_the_same_physical_identity() {
        let rule = "AWS::Events::Rule";
        let default_bus = json!({"Name": "r"});
        let other_bus = json!({"Name": "r", "EventBusName": "b"});
        // Recorded: same name on another bus replaces and succeeds.
        assert!(classify(rule, &default_bus, &other_bus).is_replacement());
        assert_eq!(
            custom_name_blocking_replacement(rule, &default_bus, &other_bus),
            None
        );
        assert_ne!(
            custom_name(rule, &default_bus),
            custom_name(rule, &other_bus)
        );
        // A scope from the primary identifier counts the same way.
        for (resource_type, old, new) in [
            (
                "AWS::ECS::Service",
                json!({"ServiceName": "s", "Cluster": "a"}),
                json!({"ServiceName": "s", "Cluster": "b"}),
            ),
            (
                "AWS::Lambda::Alias",
                json!({"Name": "live", "FunctionName": "a"}),
                json!({"Name": "live", "FunctionName": "b"}),
            ),
        ] {
            assert!(classify(resource_type, &old, &new).is_replacement());
            assert_eq!(
                custom_name_blocking_replacement(resource_type, &old, &new),
                None,
                "{resource_type}"
            );
        }
        // Same scope, same name, and a replacing change: still refused.
        assert_eq!(
            custom_name_blocking_replacement(
                "AWS::ECS::Service",
                &json!({"ServiceName": "s", "Cluster": "a", "Role": "x"}),
                &json!({"ServiceName": "s", "Cluster": "a", "Role": "y"})
            )
            .as_deref(),
            Some("s")
        );
    }

    #[test]
    fn numeric_scalars_inside_objects_compare_as_text() {
        assert!(same_value(&json!({"A": "5"}), &json!({"A": 5})));
        assert!(same_value(&json!({"A": null, "B": 1}), &json!({"B": 1})));
        assert!(!same_value(&json!([1, 2]), &json!([2, 1])));
    }
}
