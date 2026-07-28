use base64::Engine;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Internal sentinel emitted whenever a CFN value resolves to
/// `AWS::NoValue`. After resolution finishes, [`strip_no_value`] walks
/// the result and removes any object entry / array slot whose value is
/// this marker, matching CloudFormation's "drop the property" semantics
/// (e.g. inside an `Fn::If` branch). Picked so it cannot collide with
/// any real CFN property.
const NO_VALUE_SENTINEL_KEY: &str = "__fakecloud_aws_no_value__";

/// A parsed CloudFormation template.
#[derive(Debug, Clone, Default)]
pub struct ParsedTemplate {
    pub description: Option<String>,
    pub resources: Vec<ResourceDefinition>,
    pub outputs: Vec<TemplateOutput>,
}

/// Resolved Outputs entry from the template's top-level `Outputs` block.
/// `value` is the post-resolution string; `export_name` is set when the
/// output declares `Export.Name`.
#[derive(Debug, Clone)]
pub struct TemplateOutput {
    pub logical_id: String,
    pub value: String,
    pub description: Option<String>,
    pub export_name: Option<String>,
}

/// A single resource from the template.
#[derive(Debug, Clone)]
pub struct ResourceDefinition {
    pub logical_id: String,
    pub resource_type: String,
    pub properties: Value,
}

/// Known pseudo-references that should be passed through as-is.
const PSEUDO_REFS: &[&str] = &[
    "AWS::AccountId",
    "AWS::NotificationARNs",
    "AWS::NoValue",
    "AWS::Partition",
    "AWS::Region",
    "AWS::StackId",
    "AWS::StackName",
    "AWS::URLSuffix",
];

type Mappings = BTreeMap<String, BTreeMap<String, BTreeMap<String, Value>>>;

/// Map an AWS region to its IAM/ARN partition. China regions land on
/// `aws-cn`, GovCloud on `aws-us-gov`, everything else on `aws`. Used
/// by both `AWS::Partition` resolution and the URL-suffix derivation
/// below so partition decisions stay consistent.
pub(crate) fn partition_for_region(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

/// Map an AWS region to its DNS URL suffix. China regions use
/// `amazonaws.com.cn`; every other partition (commercial + GovCloud)
/// uses `amazonaws.com`, matching the real CFN `AWS::URLSuffix`.
pub(crate) fn url_suffix_for_region(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_template() {
        let template = r#"{
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": "test-queue"
                    }
                }
            }
        }"#;

        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(parsed.resources.len(), 1);
        assert_eq!(parsed.resources[0].logical_id, "MyQueue");
        assert_eq!(parsed.resources[0].resource_type, "AWS::SQS::Queue");
    }

    #[test]
    fn parse_yaml_template() {
        let template = r#"
Resources:
  MyTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: test-topic
"#;

        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(parsed.resources.len(), 1);
        assert_eq!(parsed.resources[0].logical_id, "MyTopic");
        assert_eq!(parsed.resources[0].resource_type, "AWS::SNS::Topic");
    }

    #[test]
    fn resolve_ref_parameters() {
        let template = r#"{
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": { "Ref": "QueueNameParam" }
                    }
                }
            }
        }"#;

        let mut params = BTreeMap::new();
        params.insert("QueueNameParam".to_string(), "resolved-queue".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("resolved-queue".to_string())
        );
    }

    #[test]
    fn ref_resolves_physical_id_over_logical_id() {
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": "my-topic"
                    }
                },
                "MySub": {
                    "Type": "AWS::SNS::Subscription",
                    "Properties": {
                        "TopicArn": { "Ref": "MyTopic" },
                        "Protocol": "sqs",
                        "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q"
                    }
                }
            }
        }"#;

        let mut physical_ids = BTreeMap::new();
        physical_ids.insert(
            "MyTopic".to_string(),
            "arn:aws:sns:us-east-1:123456789012:my-topic".to_string(),
        );

        let parsed =
            parse_template_with_physical_ids(template, &BTreeMap::new(), &physical_ids).unwrap();
        let sub = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "MySub")
            .unwrap();
        assert_eq!(
            sub.properties["TopicArn"],
            Value::String("arn:aws:sns:us-east-1:123456789012:my-topic".to_string())
        );
    }

    #[test]
    fn ref_without_physical_id_returns_logical_id_for_known_resource() {
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": "my-topic"
                    }
                },
                "MySub": {
                    "Type": "AWS::SNS::Subscription",
                    "Properties": {
                        "TopicArn": { "Ref": "MyTopic" },
                        "Protocol": "sqs",
                        "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q"
                    }
                }
            }
        }"#;

        // No physical IDs yet — logical ID returned for known resources
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let sub = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "MySub")
            .unwrap();
        assert_eq!(
            sub.properties["TopicArn"],
            Value::String("MyTopic".to_string())
        );
    }

    #[test]
    fn pseudo_ref_substitutes_when_param_provided() {
        let template = r#"{
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueArn": {
                            "Fn::Join": ["", [
                                "arn:", {"Ref": "AWS::Partition"}, ":sqs:",
                                {"Ref": "AWS::Region"}, ":", {"Ref": "AWS::AccountId"},
                                ":", {"Ref": "AWS::StackName"}, "-q"
                            ]]
                        }
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "us-west-2".to_string());
        params.insert("AWS::AccountId".to_string(), "111122223333".to_string());
        params.insert("AWS::Partition".to_string(), "aws".to_string());
        params.insert("AWS::StackName".to_string(), "demo".to_string());

        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueArn"],
            Value::String("arn:aws:sqs:us-west-2:111122223333:demo-q".to_string())
        );
    }

    #[test]
    fn pseudo_ref_partition_default_when_unset() {
        let template = r#"{
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "Partition": {"Ref": "AWS::Partition"},
                        "Suffix": {"Ref": "AWS::URLSuffix"}
                    }
                }
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Partition"],
            Value::String("aws".to_string())
        );
        assert_eq!(
            parsed.resources[0].properties["Suffix"],
            Value::String("amazonaws.com".to_string())
        );
    }

    #[test]
    fn pseudo_ref_passes_through() {
        let template = r#"{
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": { "Ref": "AWS::StackName" }
                    }
                }
            }
        }"#;

        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("AWS::StackName".to_string())
        );
    }

    // ── BB6: pseudo-parameter coverage ────────────────────────────

    #[test]
    fn bb6_ref_aws_region_returns_seeded_region() {
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"Region": {"Ref": "AWS::Region"}}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "us-east-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Region"],
            Value::String("us-east-1".to_string())
        );
    }

    #[test]
    fn bb6_fn_sub_substitutes_aws_account_id() {
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "Owner": {"Fn::Sub": "owner-${AWS::AccountId}"}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::AccountId".to_string(), "123456789012".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Owner"],
            Value::String("owner-123456789012".to_string())
        );
    }

    #[test]
    fn bb6_partition_for_china_region_is_aws_cn() {
        // Caller seeds region but no explicit partition; pseudo_value
        // should derive `aws-cn` for cn-* regions.
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"P": {"Ref": "AWS::Partition"}}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "cn-north-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["P"],
            Value::String("aws-cn".to_string())
        );
    }

    #[test]
    fn bb6_partition_for_govcloud_region_is_aws_us_gov() {
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"P": {"Ref": "AWS::Partition"}}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "us-gov-west-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["P"],
            Value::String("aws-us-gov".to_string())
        );
    }

    #[test]
    fn bb6_url_suffix_for_china_is_amazonaws_com_cn() {
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"S": {"Ref": "AWS::URLSuffix"}}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "cn-north-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["S"],
            Value::String("amazonaws.com.cn".to_string())
        );
    }

    #[test]
    fn bb6_url_suffix_for_govcloud_stays_amazonaws_com() {
        // GovCloud keeps the standard suffix — only China switches.
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"S": {"Ref": "AWS::URLSuffix"}}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "us-gov-east-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["S"],
            Value::String("amazonaws.com".to_string())
        );
    }

    #[test]
    fn bb6_no_value_omits_property_from_resource_input() {
        // Direct Ref to AWS::NoValue (no Fn::If wrapper) must still
        // drop the property from the resolved resource map.
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": "q",
                        "OptionalProp": {"Ref": "AWS::NoValue"}
                    }
                }
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let props = parsed.resources[0].properties.as_object().unwrap();
        assert!(
            !props.contains_key("OptionalProp"),
            "OptionalProp should be omitted, got: {props:?}"
        );
        assert_eq!(
            props.get("QueueName"),
            Some(&Value::String("q".to_string()))
        );
    }

    #[test]
    fn bb6_notification_arns_returns_seeded_array() {
        // Pseudo-parameter `AWS::NotificationARNs` resolves to an array
        // sourced from the JSON-encoded seed (matching the wiring in
        // service::create_stack).
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"Targets": {"Ref": "AWS::NotificationARNs"}}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert(
            "AWS::NotificationARNs".to_string(),
            r#"["arn:aws:sns:us-east-1:111122223333:topic"]"#.to_string(),
        );
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Targets"],
            serde_json::json!(["arn:aws:sns:us-east-1:111122223333:topic"])
        );
    }

    #[test]
    fn bb6_notification_arns_defaults_to_empty_array() {
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"Targets": {"Ref": "AWS::NotificationARNs"}}
                }
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Targets"],
            serde_json::json!([])
        );
    }

    #[test]
    fn bb6_fn_sub_array_form_substitutes_extra_vars() {
        // The array form `Fn::Sub: ["literal", {Var: ...}]` lets the
        // template pass extra bindings; pseudo-params still resolve.
        let template = r#"{
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "Path": {"Fn::Sub": ["${AWS::Region}/${Suffix}", {"Suffix": "tail"}]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("AWS::Region".to_string(), "eu-west-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Path"],
            Value::String("eu-west-1/tail".to_string())
        );
    }

    #[test]
    fn bb6_partition_helper_classifies_regions() {
        assert_eq!(partition_for_region("us-east-1"), "aws");
        assert_eq!(partition_for_region("eu-central-1"), "aws");
        assert_eq!(partition_for_region("cn-north-1"), "aws-cn");
        assert_eq!(partition_for_region("cn-northwest-1"), "aws-cn");
        assert_eq!(partition_for_region("us-gov-west-1"), "aws-us-gov");
        assert_eq!(partition_for_region("us-gov-east-1"), "aws-us-gov");
    }

    #[test]
    fn bb6_url_suffix_helper_classifies_regions() {
        assert_eq!(url_suffix_for_region("us-east-1"), "amazonaws.com");
        assert_eq!(url_suffix_for_region("us-gov-west-1"), "amazonaws.com");
        assert_eq!(url_suffix_for_region("cn-north-1"), "amazonaws.com.cn");
    }

    #[test]
    fn fn_sub_resolves_physical_ids() {
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": "my-topic"
                    }
                },
                "MyParam": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {
                        "Name": "/app/topic",
                        "Type": "String",
                        "Value": { "Fn::Sub": "Topic is ${MyTopic}" }
                    }
                }
            }
        }"#;

        let mut physical_ids = BTreeMap::new();
        physical_ids.insert(
            "MyTopic".to_string(),
            "arn:aws:sns:us-east-1:123456789012:my-topic".to_string(),
        );

        let parsed =
            parse_template_with_physical_ids(template, &BTreeMap::new(), &physical_ids).unwrap();
        let param = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "MyParam")
            .unwrap();
        assert_eq!(
            param.properties["Value"],
            Value::String("Topic is arn:aws:sns:us-east-1:123456789012:my-topic".to_string())
        );
    }

    // ── error paths ──

    #[test]
    fn parse_template_invalid_json_errors() {
        let params = BTreeMap::new();
        let result = parse_template("{not-json}", &params);
        assert!(result.is_err());
    }

    #[test]
    fn parse_template_missing_resources_errors() {
        let params = BTreeMap::new();
        let result = parse_template(r#"{"Description":"no resources"}"#, &params);
        assert!(result.is_err());
    }

    #[test]
    fn parse_template_resources_not_object_errors() {
        let params = BTreeMap::new();
        let result = parse_template(r#"{"Resources": []}"#, &params);
        assert!(result.is_err());
    }

    #[test]
    fn parse_template_missing_type_errors() {
        let params = BTreeMap::new();
        let result = parse_template(r#"{"Resources":{"R":{"Properties":{}}}}"#, &params);
        assert!(result.is_err());
    }

    // ── Fn::GetAtt ──

    #[test]
    fn fn_getatt_resolves_attribute_in_array_form() {
        let template = r#"{
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": { "QueueName": "q1" }
                },
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": "t1",
                        "DataProtectionPolicy": {
                            "Fn::GetAtt": ["MyQueue", "Arn"]
                        }
                    }
                }
            }
        }"#;

        let mut attrs = BTreeMap::new();
        let mut q_attrs = BTreeMap::new();
        q_attrs.insert(
            "Arn".to_string(),
            "arn:aws:sqs:us-east-1:123456789012:q1".to_string(),
        );
        attrs.insert("MyQueue".to_string(), q_attrs);

        let parsed =
            parse_template_with_resolution(template, &BTreeMap::new(), &BTreeMap::new(), &attrs)
                .unwrap();
        let topic = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "MyTopic")
            .unwrap();
        assert_eq!(
            topic.properties["DataProtectionPolicy"],
            Value::String("arn:aws:sqs:us-east-1:123456789012:q1".to_string())
        );
    }

    #[test]
    fn fn_getatt_resolves_attribute_in_short_string_form() {
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": "t1",
                        "PolicyArn": { "Fn::GetAtt": "MyQueue.Arn" }
                    }
                }
            }
        }"#;

        let mut attrs = BTreeMap::new();
        let mut q_attrs = BTreeMap::new();
        q_attrs.insert(
            "Arn".to_string(),
            "arn:aws:sqs:us-east-1:123456789012:q1".to_string(),
        );
        attrs.insert("MyQueue".to_string(), q_attrs);

        let parsed =
            parse_template_with_resolution(template, &BTreeMap::new(), &BTreeMap::new(), &attrs)
                .unwrap();
        assert_eq!(
            parsed.resources[0].properties["PolicyArn"],
            Value::String("arn:aws:sqs:us-east-1:123456789012:q1".to_string())
        );
    }

    #[test]
    fn fn_getatt_unknown_resource_returns_placeholder() {
        let template = r#"{
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": { "Fn::GetAtt": ["MyQueue", "Arn"] }
                    }
                }
            }
        }"#;

        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        // Unresolved GetAtt becomes a placeholder; multi-pass provisioning
        // re-resolves once the target is known.
        assert_eq!(
            parsed.resources[0].properties["TopicName"],
            Value::String("MyQueue.Arn".to_string())
        );
    }

    #[test]
    fn fn_getatt_inside_fn_join_resolves() {
        let template = r#"{
            "Resources": {
                "MyParam": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {
                        "Name": "/app/q",
                        "Type": "String",
                        "Value": {
                            "Fn::Join": [":", ["queue", { "Fn::GetAtt": ["MyQueue", "Arn"] }]]
                        }
                    }
                }
            }
        }"#;

        let mut attrs = BTreeMap::new();
        let mut q_attrs = BTreeMap::new();
        q_attrs.insert(
            "Arn".to_string(),
            "arn:aws:sqs:us-east-1:123456789012:q1".to_string(),
        );
        attrs.insert("MyQueue".to_string(), q_attrs);

        let parsed =
            parse_template_with_resolution(template, &BTreeMap::new(), &BTreeMap::new(), &attrs)
                .unwrap();
        assert_eq!(
            parsed.resources[0].properties["Value"],
            Value::String("queue:arn:aws:sqs:us-east-1:123456789012:q1".to_string())
        );
    }

    #[test]
    fn fn_sub_resolves_getatt_style_substitution() {
        let template = r#"{
            "Resources": {
                "MyParam": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {
                        "Name": "/app/q",
                        "Type": "String",
                        "Value": { "Fn::Sub": "Queue arn is ${MyQueue.Arn}" }
                    }
                }
            }
        }"#;

        let mut attrs = BTreeMap::new();
        let mut q_attrs = BTreeMap::new();
        q_attrs.insert(
            "Arn".to_string(),
            "arn:aws:sqs:us-east-1:123456789012:q1".to_string(),
        );
        attrs.insert("MyQueue".to_string(), q_attrs);

        let parsed =
            parse_template_with_resolution(template, &BTreeMap::new(), &BTreeMap::new(), &attrs)
                .unwrap();
        assert_eq!(
            parsed.resources[0].properties["Value"],
            Value::String("Queue arn is arn:aws:sqs:us-east-1:123456789012:q1".to_string())
        );
    }

    #[test]
    fn parse_template_with_description() {
        let params = BTreeMap::new();
        let parsed = parse_template(
            r#"{"Description":"My template","Resources":{"R":{"Type":"AWS::SQS::Queue"}}}"#,
            &params,
        )
        .unwrap();
        assert_eq!(parsed.description.as_deref(), Some("My template"));
        assert_eq!(parsed.resources.len(), 1);
    }

    type EmptyCtx = (
        BTreeMap<String, String>,
        serde_json::Map<String, Value>,
        BTreeMap<String, String>,
        BTreeMap<String, BTreeMap<String, String>>,
    );

    fn empty() -> EmptyCtx {
        (
            BTreeMap::new(),
            serde_json::Map::new(),
            BTreeMap::new(),
            BTreeMap::new(),
        )
    }

    #[test]
    fn fn_base64_encodes_string() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Base64": "hello"}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::String("aGVsbG8=".to_string()));
    }

    #[test]
    fn ref_on_lambda_alias_resolves_to_alias_arn() {
        // `Ref` returns a resource's physical id; the Lambda alias
        // provisioner exposes the alias ARN as its physical id, so
        // `Ref` on the alias yields the ARN — like real CloudFormation,
        // and what the ESM create path needs to find the function.
        let (p, r, mut ids, attrs) = empty();
        let alias_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func:live";
        ids.insert("Alias".to_string(), alias_arn.to_string());
        let v: Value = serde_json::from_str(r#"{"Ref": "Alias"}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::String(alias_arn.to_string()));
    }

    #[test]
    fn fn_split_emits_array() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Split": [",", "a,b,c"]}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, serde_json::json!(["a", "b", "c"]));
    }

    #[test]
    fn fn_select_picks_index() {
        let (p, r, ids, attrs) = empty();
        let v: Value =
            serde_json::from_str(r#"{"Fn::Select": [1, {"Fn::Split": [",", "a,b,c"]}]}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::String("b".to_string()));
    }

    #[test]
    fn fn_getazs_returns_region_azs() {
        // bug-audit 2026-07-28 (cycle 7) C1: Fn::GetAZs was unimplemented, so
        // the canonical `!Select [0, !GetAZs ""]` subnet idiom fed Select an
        // object and yielded null. GetAZs must resolve to the region's AZ list.
        let (mut p, r, ids, attrs) = empty();
        p.insert("AWS::Region".to_string(), "us-west-2".to_string());
        let v: Value = serde_json::from_str(r#"{"Fn::GetAZs": ""}"#).unwrap();
        assert_eq!(
            resolve_refs(&v, &p, &r, &ids, &attrs),
            serde_json::json!(["us-west-2a", "us-west-2b", "us-west-2c"])
        );
        // The ubiquitous Select-over-GetAZs pattern now resolves.
        let sel: Value =
            serde_json::from_str(r#"{"Fn::Select": [1, {"Fn::GetAZs": ""}]}"#).unwrap();
        assert_eq!(
            resolve_refs(&sel, &p, &r, &ids, &attrs),
            Value::String("us-west-2b".to_string())
        );
    }

    #[test]
    fn fn_getazs_explicit_region() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::GetAZs": "eu-west-1"}"#).unwrap();
        assert_eq!(
            resolve_refs(&v, &p, &r, &ids, &attrs),
            serde_json::json!(["eu-west-1a", "eu-west-1b", "eu-west-1c"])
        );
    }

    #[test]
    fn fn_sub_unescapes_literal_variable() {
        // bug-audit 2026-07-28 (cycle 7) C2: `${!X}` is CFN's escape for a
        // literal `${X}` (used to protect IAM policy variables). The render loop
        // never stripped the `!`, corrupting the policy variable. `${!X}` must
        // render `${X}`.
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Sub": "arn ${!aws:username} end"}"#).unwrap();
        assert_eq!(
            resolve_refs(&v, &p, &r, &ids, &attrs),
            Value::String("arn ${aws:username} end".to_string())
        );
    }

    #[test]
    fn fn_length_counts_array() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Length": [1,2,3,4]}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::Number(4.into()));
    }

    #[test]
    fn fn_to_json_string_serializes() {
        let (p, r, ids, attrs) = empty();
        let v: Value =
            serde_json::from_str(r#"{"Fn::ToJsonString": {"a": 1, "b": [2, 3]}}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        let s = resolved.as_str().unwrap();
        // Order-insensitive: just verify it parses back.
        let parsed: Value = serde_json::from_str(s).unwrap();
        assert_eq!(parsed["a"], serde_json::json!(1));
        assert_eq!(parsed["b"], serde_json::json!([2, 3]));
    }

    #[test]
    fn fn_cidr_carves_subnets() {
        let (p, r, ids, attrs) = empty();
        // Carve 10.0.0.0/16 into 4 /24 subnets (cidr_bits = 8 host bits).
        let v: Value = serde_json::from_str(r#"{"Fn::Cidr": ["10.0.0.0/16", 4, 8]}"#).unwrap();
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(
            resolved,
            serde_json::json!(["10.0.0.0/24", "10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24",])
        );
    }

    #[test]
    fn condition_skips_resource_when_false() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
            },
            "Resources": {
                "ProdQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "IsProd",
                    "Properties": {"QueueName": "prod-q"}
                },
                "AlwaysQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "always-q"}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "dev".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let names: Vec<&str> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.as_str())
            .collect();
        assert!(names.contains(&"AlwaysQueue"));
        assert!(!names.contains(&"ProdQueue"));
    }

    #[test]
    fn condition_includes_resource_when_true() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
            },
            "Resources": {
                "ProdQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "IsProd",
                    "Properties": {"QueueName": "prod-q"}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "prod".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(parsed.resources.len(), 1);
    }

    #[test]
    fn fn_if_picks_branch_based_on_condition() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::If": ["IsProd", "prod-q", "dev-q"]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "dev".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("dev-q".to_string())
        );
    }

    #[test]
    fn fn_and_or_not_combine_conditions() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}, "Region": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                "IsUsEast": {"Fn::Equals": [{"Ref": "Region"}, "us-east-1"]},
                "IsProdInUsEast": {"Fn::And": [{"Condition": "IsProd"}, {"Condition": "IsUsEast"}]},
                "IsNotProd": {"Fn::Not": [{"Condition": "IsProd"}]},
                "IsAny": {"Fn::Or": [{"Condition": "IsProd"}, {"Condition": "IsNotProd"}]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "P1": {"Fn::If": ["IsProdInUsEast", "yes", "no"]},
                        "P2": {"Fn::If": ["IsNotProd", "yes", "no"]},
                        "P3": {"Fn::If": ["IsAny", "yes", "no"]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "prod".to_string());
        params.insert("Region".to_string(), "us-east-1".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let p = &parsed.resources[0].properties;
        assert_eq!(p["P1"], Value::String("yes".to_string()));
        assert_eq!(p["P2"], Value::String("no".to_string()));
        assert_eq!(p["P3"], Value::String("yes".to_string()));
    }

    #[test]
    fn fn_find_in_map_resolves_leaf_value() {
        let template = r#"{
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"},
                    "us-west-2": {"AMI": "ami-west"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": ["RegionMap", "us-east-1", "AMI"]}
                    }
                }
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-east".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_resolves_keys_via_ref() {
        let template = r#"{
            "Parameters": {"Region": {"Type": "String"}},
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"},
                    "us-west-2": {"AMI": "ami-west"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": ["RegionMap", {"Ref": "Region"}, "AMI"]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Region".to_string(), "us-west-2".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-west".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_unknown_keys_returns_error() {
        let template = r#"{
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": ["RegionMap", "ap-south-1", "AMI"]}
                    }
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(
            err.contains("Unable to get mapping for RegionMap::ap-south-1::AMI"),
            "got: {err}"
        );
    }

    #[test]
    fn fn_find_in_map_four_arg_returns_default_when_missing() {
        let template = r#"{
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": [
                            "RegionMap",
                            "ap-south-1",
                            "AMI",
                            {"DefaultValue": "ami-fallback"}
                        ]}
                    }
                }
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-fallback".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_four_arg_prefers_match_over_default() {
        let template = r#"{
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": [
                            "RegionMap",
                            "us-east-1",
                            "AMI",
                            {"DefaultValue": "ami-fallback"}
                        ]}
                    }
                }
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-east".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_default_value_is_resolved_intrinsic() {
        let template = r#"{
            "Parameters": {"Fallback": {"Type": "String"}},
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": [
                            "RegionMap",
                            "ap-south-1",
                            "AMI",
                            {"DefaultValue": {"Ref": "Fallback"}}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Fallback".to_string(), "ami-default".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-default".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_unknown_map_name_errors() {
        let template = r#"{
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": ["DoesNotExist", "us-east-1", "AMI"]}
                    }
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(
            err.contains("Unable to get mapping for DoesNotExist::us-east-1::AMI"),
            "got: {err}"
        );
    }

    #[test]
    fn fn_find_in_map_wrong_arg_count_errors() {
        let template = r#"{
            "Mappings": {"M": {"a": {"b": "c"}}},
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::FindInMap": ["M", "a"]}
                    }
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(
            err.contains("Fn::FindInMap requires 3 or 4 arguments"),
            "got: {err}"
        );
    }

    #[test]
    fn fn_find_in_map_resolves_via_pseudo_region() {
        let template = r#"{
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"},
                    "us-west-2": {"AMI": "ami-west"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::FindInMap": [
                            "RegionMap",
                            {"Ref": "AWS::Region"},
                            "AMI"
                        ]}
                    }
                }
            }
        }"#;
        // No AWS::Region in parameters — the pseudo-default ("us-east-1")
        // should kick in so FindInMap still resolves.
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-east".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_in_unused_if_branch_does_not_error() {
        // FindInMap sits in the FALSE branch of Fn::If; the path
        // `RegionMap::ap-south-1::AMI` doesn't exist. Because
        // `WantAlt` resolves to "no" the alt branch is unused and
        // CFN never executes that FindInMap — parse_template must
        // succeed instead of erroring.
        let template = r#"{
            "Parameters": {"WantAlt": {"Type": "String"}},
            "Conditions": {
                "UseAlt": {"Fn::Equals": [{"Ref": "WantAlt"}, "yes"]}
            },
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::If": [
                            "UseAlt",
                            {"Fn::FindInMap": ["RegionMap", "ap-south-1", "AMI"]},
                            {"Fn::FindInMap": ["RegionMap", "us-east-1", "AMI"]}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("WantAlt".to_string(), "no".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["ImageId"],
            Value::String("ami-east".to_string())
        );
    }

    #[test]
    fn fn_find_in_map_in_active_if_branch_still_errors_on_miss() {
        // Same shape as above but the active branch is the broken
        // one; the strict miss handling must still surface.
        let template = r#"{
            "Parameters": {"WantAlt": {"Type": "String"}},
            "Conditions": {
                "UseAlt": {"Fn::Equals": [{"Ref": "WantAlt"}, "yes"]}
            },
            "Mappings": {
                "RegionMap": {
                    "us-east-1": {"AMI": "ami-east"}
                }
            },
            "Resources": {
                "Inst": {
                    "Type": "AWS::EC2::Instance",
                    "Properties": {
                        "ImageId": {"Fn::If": [
                            "UseAlt",
                            {"Fn::FindInMap": ["RegionMap", "ap-south-1", "AMI"]},
                            {"Fn::FindInMap": ["RegionMap", "us-east-1", "AMI"]}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("WantAlt".to_string(), "yes".to_string());
        let err = parse_template(template, &params).unwrap_err();
        assert!(
            err.contains("Unable to get mapping for RegionMap::ap-south-1::AMI"),
            "got: {err}"
        );
    }

    #[test]
    fn fn_find_in_map_alongside_ref_and_sub_still_resolve() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Mappings": {
                "EnvMap": {
                    "prod": {"Suffix": "live"},
                    "dev": {"Suffix": "test"}
                }
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::FindInMap": ["EnvMap", {"Ref": "Env"}, "Suffix"]},
                        "Tags": [
                            {"Key": "EnvRef", "Value": {"Ref": "Env"}},
                            {"Key": "Subbed", "Value": {"Fn::Sub": "env-${Env}"}}
                        ]
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "prod".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let p = &parsed.resources[0].properties;
        assert_eq!(p["QueueName"], Value::String("live".to_string()));
        assert_eq!(p["Tags"][0]["Value"], Value::String("prod".to_string()));
        assert_eq!(p["Tags"][1]["Value"], Value::String("env-prod".to_string()));
    }

    // ── Conditions: cycle detection + AWS::NoValue removal ──

    #[test]
    fn cyclic_conditions_self_reference_errors() {
        let template = r#"{
            "Conditions": {
                "A": {"Condition": "A"}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "A",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(err.contains("Circular reference"), "got: {err}");
        assert!(err.contains("'A'"), "got: {err}");
    }

    #[test]
    fn cyclic_conditions_two_step_errors() {
        let template = r#"{
            "Conditions": {
                "A": {"Condition": "B"},
                "B": {"Condition": "A"}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "A",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(err.contains("Circular reference"), "got: {err}");
    }

    #[test]
    fn condition_referencing_undefined_name_errors() {
        let template = r#"{
            "Conditions": {
                "A": {"Condition": "DoesNotExist"}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "A",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(err.contains("DoesNotExist"), "got: {err}");
    }

    #[test]
    fn fn_if_no_value_removes_property_from_parent_map() {
        let template = r#"{
            "Parameters": {"WantTags": {"Type": "String"}},
            "Conditions": {
                "HasTags": {"Fn::Equals": [{"Ref": "WantTags"}, "yes"]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": "q",
                        "Tags": {"Fn::If": [
                            "HasTags",
                            [{"Key": "a", "Value": "b"}],
                            {"Ref": "AWS::NoValue"}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("WantTags".to_string(), "no".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let props = parsed.resources[0].properties.as_object().unwrap();
        assert!(
            !props.contains_key("Tags"),
            "Tags should be omitted when AWS::NoValue picked, got: {props:?}"
        );
        assert_eq!(
            props.get("QueueName"),
            Some(&Value::String("q".to_string()))
        );
    }

    #[test]
    fn fn_if_no_value_keeps_property_when_branch_concrete() {
        let template = r#"{
            "Parameters": {"WantTags": {"Type": "String"}},
            "Conditions": {
                "HasTags": {"Fn::Equals": [{"Ref": "WantTags"}, "yes"]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": "q",
                        "Tags": {"Fn::If": [
                            "HasTags",
                            [{"Key": "a", "Value": "b"}],
                            {"Ref": "AWS::NoValue"}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("WantTags".to_string(), "yes".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let tags = &parsed.resources[0].properties["Tags"];
        assert_eq!(
            tags,
            &serde_json::json!([{"Key": "a", "Value": "b"}]),
            "tags should be the true branch's array"
        );
    }

    #[test]
    fn fn_if_no_value_in_array_drops_element() {
        let template = r#"{
            "Parameters": {"Extra": {"Type": "String"}},
            "Conditions": {
                "HasExtra": {"Fn::Equals": [{"Ref": "Extra"}, "yes"]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "Items": [
                            "first",
                            {"Fn::If": ["HasExtra", "second", {"Ref": "AWS::NoValue"}]},
                            "third"
                        ]
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Extra".to_string(), "no".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["Items"],
            serde_json::json!(["first", "third"])
        );
    }

    #[test]
    fn condition_skips_output_when_false() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "q"}
                }
            },
            "Outputs": {
                "ProdName": {
                    "Condition": "IsProd",
                    "Value": "prod-only"
                },
                "Always": {
                    "Value": "shown"
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "dev".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let names: Vec<&str> = parsed
            .outputs
            .iter()
            .map(|o| o.logical_id.as_str())
            .collect();
        assert!(names.contains(&"Always"));
        assert!(!names.contains(&"ProdName"));
    }

    #[test]
    fn fn_and_short_circuits_on_false() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                "Combined": {"Fn::And": [
                    {"Condition": "IsProd"},
                    {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
                ]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "Combined",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "dev".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(parsed.resources.len(), 0);
    }

    #[test]
    fn fn_or_short_circuits_on_true() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                "AnyEnv": {"Fn::Or": [
                    {"Condition": "IsProd"},
                    {"Fn::Equals": [{"Ref": "Env"}, "dev"]},
                    {"Fn::Equals": [{"Ref": "Env"}, "stage"]}
                ]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "AnyEnv",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "stage".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(parsed.resources.len(), 1);
    }

    #[test]
    fn fn_and_rejects_arity_outside_1_to_10() {
        let template = r#"{
            "Conditions": {
                "Empty": {"Fn::And": []}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "Empty",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(err.contains("Fn::And"), "got: {err}");
    }

    #[test]
    fn condition_evaluation_memoizes_complex_expression() {
        // Both `Outer` branches reuse `Inner`. With memoization the
        // inner condition only resolves once; without it, this would
        // still pass — but the test guards against regressions where
        // re-evaluation triggers double Fn::Equals work.
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "Inner": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                "OuterA": {"Fn::And": [{"Condition": "Inner"}, {"Condition": "Inner"}]},
                "OuterB": {"Fn::Or": [{"Condition": "Inner"}, {"Condition": "OuterA"}]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "OuterB",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "prod".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(parsed.resources.len(), 1);
    }

    #[test]
    fn fn_not_rejects_multiple_arguments() {
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Conditions": {
                "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                "Bad": {"Fn::Not": [
                    {"Condition": "IsProd"},
                    {"Condition": "IsProd"}
                ]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "Bad",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "prod".to_string());
        let err = parse_template(template, &params).unwrap_err();
        assert!(err.contains("Fn::Not"), "got: {err}");
    }

    #[test]
    fn fn_not_rejects_zero_arguments() {
        let template = r#"{
            "Conditions": {
                "Bad": {"Fn::Not": []}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Condition": "Bad",
                    "Properties": {"QueueName": "q"}
                }
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(err.contains("Fn::Not"), "got: {err}");
    }

    #[test]
    fn resolve_resource_properties_strips_no_value_at_provision_time() {
        // Mirrors the incremental-provisioning code path which calls
        // resolve_resource_properties_with_attrs after the initial parse.
        // The sentinel must not leak into the resolved properties even
        // when re-resolved with updated physical IDs.
        let template = r#"{
            "Parameters": {"WantTags": {"Type": "String"}},
            "Conditions": {
                "HasTags": {"Fn::Equals": [{"Ref": "WantTags"}, "yes"]}
            },
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": "q",
                        "Tags": {"Fn::If": [
                            "HasTags",
                            [{"Key": "a", "Value": "b"}],
                            {"Ref": "AWS::NoValue"}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("WantTags".to_string(), "no".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let resource = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "Q")
            .unwrap();
        // First parse already strips Tags.
        assert!(!resource
            .properties
            .as_object()
            .unwrap()
            .contains_key("Tags"));

        // Re-resolve with empty physical IDs (mid-provisioning). The
        // sentinel must still be stripped — no `__fakecloud_aws_no_value__`
        // marker should reach the caller.
        let reresolved = resolve_resource_properties_with_attrs(
            resource,
            template,
            &params,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();
        let props = reresolved.properties.as_object().unwrap();
        assert!(
            !props.contains_key("Tags"),
            "Tags should be stripped on re-resolve, got: {props:?}"
        );
        // Sanity: serialized form must not contain the sentinel key.
        let serialized = serde_json::to_string(&reresolved.properties).unwrap();
        assert!(
            !serialized.contains(NO_VALUE_SENTINEL_KEY),
            "sentinel leaked: {serialized}"
        );
    }

    // ── BB5: Fn::Select / Split / Base64 / Cidr / Length / ToJsonString / ForEach ──

    #[test]
    fn fn_select_string_index_resolves() {
        // CFN accepts the index as a string literal (`"0"`) — CFN's
        // own examples do this, so the engine must coerce.
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Select": ["2", ["a", "b", "c", "d"]]}"#)
            .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::String("c".to_string()));
    }

    #[test]
    fn fn_select_out_of_range_returns_null() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Select": [10, ["a", "b"]]}"#)
            .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::Null);
    }

    #[test]
    fn fn_select_over_json_array_string_getatt_resolves_as_list() {
        // A list-valued Fn::GetAtt (e.g. AWS::AmazonMQ::Broker AmqpEndpoints)
        // resolves to a JSON-array *string* because attributes are String-keyed;
        // Fn::Select must parse it back into a list and index into it rather than
        // collapsing to the first element.
        let (p, r, ids, mut attrs) = empty();
        let mut broker_attrs = BTreeMap::new();
        broker_attrs.insert(
            "AmqpEndpoints".to_string(),
            r#"["amqps://a.example:5671","amqps://b.example:5671"]"#.to_string(),
        );
        attrs.insert("MyBroker".to_string(), broker_attrs);
        let v: Value = serde_json::from_str(
            r#"{"Fn::Select": [1, {"Fn::GetAtt": ["MyBroker", "AmqpEndpoints"]}]}"#,
        )
        .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(
            resolved,
            Value::String("amqps://b.example:5671".to_string())
        );
    }

    #[test]
    fn fn_select_resolves_ref_inside_list() {
        let template = r#"{
            "Parameters": {"AZs": {"Type": "CommaDelimitedList"}},
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::Select": [0, {"Fn::Split": [",", {"Ref": "AZs"}]}]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert(
            "AZs".to_string(),
            "us-east-1a,us-east-1b,us-east-1c".to_string(),
        );
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("us-east-1a".to_string())
        );
    }

    #[test]
    fn fn_split_empty_delimiter_returns_full_string_split_per_char() {
        let (p, r, ids, attrs) = empty();
        let v: Value =
            serde_json::from_str(r#"{"Fn::Split": ["", "abc"]}"#).expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        // str::split("") yields `["", "a", "b", "c", ""]` in Rust.
        // CFN's behavior with empty delimiter is undefined, but match
        // the underlying primitive so callers can reason about it.
        assert!(resolved.is_array());
    }

    #[test]
    fn fn_split_no_match_returns_single_element_array() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Split": [",", "no-commas-here"]}"#)
            .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, serde_json::json!(["no-commas-here"]));
    }

    #[test]
    fn fn_base64_encodes_unicode() {
        let (p, r, ids, attrs) = empty();
        let v: Value =
            serde_json::from_str(r#"{"Fn::Base64": "héllo"}"#).expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        // "héllo" is 6 bytes UTF-8 (h=1, é=2, l=1, l=1, o=1).
        assert_eq!(resolved, Value::String("aMOpbGxv".to_string()));
    }

    #[test]
    fn fn_base64_resolves_nested_intrinsic() {
        let template = r#"{
            "Parameters": {"Greeting": {"Type": "String"}},
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::Base64": {"Ref": "Greeting"}}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Greeting".to_string(), "hello".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("aGVsbG8=".to_string())
        );
    }

    #[test]
    fn fn_length_counts_string_chars() {
        let (p, r, ids, attrs) = empty();
        let v: Value =
            serde_json::from_str(r#"{"Fn::Length": "héllo"}"#).expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        // 5 chars (not 6 bytes) — multibyte counted once.
        assert_eq!(resolved, Value::Number(5.into()));
    }

    #[test]
    fn fn_length_resolves_nested_split() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Length": {"Fn::Split": [",", "a,b,c,d,e"]}}"#)
            .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::Number(5.into()));
    }

    #[test]
    fn fn_to_json_string_serializes_array() {
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::ToJsonString": ["a", "b", "c"]}"#)
            .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, Value::String(r#"["a","b","c"]"#.to_string()));
    }

    #[test]
    fn fn_to_json_string_resolves_inner_ref() {
        let template = r#"{
            "Parameters": {"Name": {"Type": "String"}},
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {
                            "Fn::ToJsonString": {"k": {"Ref": "Name"}}
                        }
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Name".to_string(), "abc".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String(r#"{"k":"abc"}"#.to_string())
        );
    }

    #[test]
    fn fn_cidr_count_matches_request() {
        // Real Fn::Cidr returns up to 2^cidr_bits subnets; we ask for 2
        // out of a possible 256, so only 2 land in the output.
        let (p, r, ids, attrs) = empty();
        let v: Value = serde_json::from_str(r#"{"Fn::Cidr": ["10.0.0.0/16", 2, 8]}"#)
            .expect("static fixture parses");
        let resolved = resolve_refs(&v, &p, &r, &ids, &attrs);
        assert_eq!(resolved, serde_json::json!(["10.0.0.0/24", "10.0.1.0/24"]));
    }

    #[test]
    fn fn_cidr_resolves_via_ref() {
        let template = r#"{
            "Parameters": {"Vpc": {"Type": "String"}},
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::Select": [
                            0,
                            {"Fn::Cidr": [{"Ref": "Vpc"}, 4, 8]}
                        ]}
                    }
                }
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Vpc".to_string(), "172.16.0.0/16".to_string());
        let parsed = parse_template(template, &params).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("172.16.0.0/24".to_string())
        );
    }

    #[test]
    fn fn_for_each_expands_resources() {
        let template = r#"{
            "Resources": {
                "Fn::ForEach::TopicLoop": [
                    "TopicName",
                    ["alpha", "beta", "gamma"],
                    {
                        "${TopicName}Topic": {
                            "Type": "AWS::SNS::Topic",
                            "Properties": {"TopicName": "${TopicName}-topic"}
                        }
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let names: Vec<&str> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.as_str())
            .collect();
        assert!(names.contains(&"alphaTopic"), "got: {names:?}");
        assert!(names.contains(&"betaTopic"), "got: {names:?}");
        assert!(names.contains(&"gammaTopic"), "got: {names:?}");
        let alpha = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "alphaTopic")
            .unwrap();
        assert_eq!(
            alpha.properties["TopicName"],
            Value::String("alpha-topic".to_string())
        );
    }

    #[test]
    fn fn_for_each_substitutes_in_nested_values() {
        let template = r#"{
            "Resources": {
                "Fn::ForEach::Q": [
                    "QName",
                    ["one", "two"],
                    {
                        "${QName}Queue": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {
                                "QueueName": "${QName}",
                                "Tags": [
                                    {"Key": "name", "Value": "${QName}"}
                                ]
                            }
                        }
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let one = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "oneQueue")
            .unwrap();
        assert_eq!(
            one.properties["QueueName"],
            Value::String("one".to_string())
        );
        assert_eq!(
            one.properties["Tags"][0]["Value"],
            Value::String("one".to_string())
        );
    }

    #[test]
    fn fn_for_each_nested_loops_expand_cartesian() {
        let template = r#"{
            "Resources": {
                "Fn::ForEach::Outer": [
                    "Env",
                    ["dev", "prod"],
                    {
                        "Fn::ForEach::Inner": [
                            "Region",
                            ["us-east-1", "eu-west-1"],
                            {
                                "${Env}${Region}Q": {
                                    "Type": "AWS::SQS::Queue",
                                    "Properties": {"QueueName": "${Env}-${Region}"}
                                }
                            }
                        ]
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let names: Vec<&str> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.as_str())
            .collect();
        for env in ["dev", "prod"] {
            for region in ["us-east-1", "eu-west-1"] {
                let expected = format!("{env}{region}Q");
                assert!(
                    names.contains(&expected.as_str()),
                    "missing {expected} in {names:?}"
                );
            }
        }
        let dev_us = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "devus-east-1Q")
            .unwrap();
        assert_eq!(
            dev_us.properties["QueueName"],
            Value::String("dev-us-east-1".to_string())
        );
    }

    #[test]
    fn fn_for_each_keeps_other_resources_untouched() {
        let template = r#"{
            "Resources": {
                "Static": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "static-q"}
                },
                "Fn::ForEach::Loop": [
                    "I",
                    ["a", "b"],
                    {
                        "${I}Topic": {
                            "Type": "AWS::SNS::Topic",
                            "Properties": {"TopicName": "${I}"}
                        }
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let names: Vec<&str> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.as_str())
            .collect();
        assert!(names.contains(&"Static"));
        assert!(names.contains(&"aTopic"));
        assert!(names.contains(&"bTopic"));
        assert_eq!(parsed.resources.len(), 3);
    }

    #[test]
    fn fn_for_each_invalid_arity_errors() {
        let template = r#"{
            "Resources": {
                "Fn::ForEach::Bad": [
                    "Var",
                    ["a"]
                ]
            }
        }"#;
        let err = parse_template(template, &BTreeMap::new()).unwrap_err();
        assert!(err.contains("Fn::ForEach"), "got: {err}");
    }

    #[test]
    fn fn_for_each_resolves_intrinsics_in_emitted_resources() {
        // Body of the loop references both the loop variable and a
        // stack parameter, exercising that downstream intrinsic
        // resolution still runs over emitted resources.
        let template = r#"{
            "Parameters": {"Env": {"Type": "String"}},
            "Resources": {
                "Fn::ForEach::Q": [
                    "Name",
                    ["alpha", "beta"],
                    {
                        "${Name}Queue": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {
                                "QueueName": {"Fn::Sub": "${Env}-${Name}"}
                            }
                        }
                    }
                ]
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Env".to_string(), "prod".to_string());
        let parsed = parse_template(template, &params).unwrap();
        // ${Name} substitutes at ForEach expansion time; ${Env} comes
        // from the parameter at Fn::Sub time. Both must land.
        let alpha = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "alphaQueue")
            .unwrap();
        assert_eq!(
            alpha.properties["QueueName"],
            Value::String("prod-alpha".to_string())
        );
    }

    #[test]
    fn fn_for_each_re_resolves_at_provision_time() {
        // resolve_resource_properties_with_attrs must also expand
        // ForEach so the looked-up resource by logical ID matches the
        // post-expansion template.
        let template = r#"{
            "Resources": {
                "Fn::ForEach::Q": [
                    "Name",
                    ["alpha"],
                    {
                        "${Name}Queue": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {"QueueName": "${Name}-q"}
                        }
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let resource = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "alphaQueue")
            .unwrap();
        let reresolved = resolve_resource_properties_with_attrs(
            resource,
            template,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            reresolved.properties["QueueName"],
            Value::String("alpha-q".to_string())
        );
    }

    #[test]
    fn fn_for_each_resolves_ref_to_comma_delimited_list_param() {
        // CommaDelimitedList parameters are a documented ForEach input
        // shape. Stack passes the value as a single string; ForEach
        // must split it before iterating.
        let template = r#"{
            "Parameters": {"Names": {"Type": "CommaDelimitedList"}},
            "Resources": {
                "Fn::ForEach::Q": [
                    "N",
                    {"Ref": "Names"},
                    {
                        "${N}Queue": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {"QueueName": "${N}-q"}
                        }
                    }
                ]
            }
        }"#;
        let mut params = BTreeMap::new();
        params.insert("Names".to_string(), "alpha,beta,gamma".to_string());
        let parsed = parse_template(template, &params).unwrap();
        let names: Vec<&str> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.as_str())
            .collect();
        for v in ["alphaQueue", "betaQueue", "gammaQueue"] {
            assert!(names.contains(&v), "missing {v} in {names:?}");
        }
    }

    #[test]
    fn fn_for_each_ampersand_substitution_form() {
        // AWS supports `&{Var}` in addition to `${Var}` for ForEach
        // loop variable substitution; needed when the surrounding
        // template separately uses ${}-style for Fn::Sub.
        let template = r#"{
            "Resources": {
                "Fn::ForEach::Q": [
                    "Name",
                    ["alpha", "beta"],
                    {
                        "&{Name}Queue": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {"QueueName": "&{Name}"}
                        }
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let names: Vec<&str> = parsed
            .resources
            .iter()
            .map(|r| r.logical_id.as_str())
            .collect();
        assert!(names.contains(&"alphaQueue"), "got: {names:?}");
        assert!(names.contains(&"betaQueue"), "got: {names:?}");
        let alpha = parsed
            .resources
            .iter()
            .find(|r| r.logical_id == "alphaQueue")
            .unwrap();
        assert_eq!(
            alpha.properties["QueueName"],
            Value::String("alpha".to_string())
        );
    }

    #[test]
    fn fn_for_each_in_outputs_expands() {
        let template = r#"{
            "Resources": {
                "Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "q"}}
            },
            "Outputs": {
                "Fn::ForEach::OutputLoop": [
                    "I",
                    ["one", "two"],
                    {
                        "${I}Out": {"Value": "${I}-value"}
                    }
                ]
            }
        }"#;
        let parsed = parse_template(template, &BTreeMap::new()).unwrap();
        let names: Vec<&str> = parsed
            .outputs
            .iter()
            .map(|o| o.logical_id.as_str())
            .collect();
        assert!(names.contains(&"oneOut"), "got: {names:?}");
        assert!(names.contains(&"twoOut"), "got: {names:?}");
        let one = parsed
            .outputs
            .iter()
            .find(|o| o.logical_id == "oneOut")
            .unwrap();
        assert_eq!(one.value, "one-value");
    }
}

mod conditions;
mod for_each;
mod intrinsics;
mod mappings;
mod parser;
mod resolution;
mod sam_events;
use conditions::*;
use for_each::*;
use intrinsics::*;
use mappings::*;

pub use parser::{
    collect_import_value_names, parse_outputs, parse_template, parse_template_with_physical_ids,
    parse_template_with_resolution,
};
pub use resolution::{
    dependency_order, resolve_resource_properties, resolve_resource_properties_with_attrs,
};
