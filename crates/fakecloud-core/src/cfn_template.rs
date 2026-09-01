//! Parsing for CloudFormation-shaped template bodies.
//!
//! A template body arrives as either JSON or YAML, and the YAML dialect
//! CloudFormation accepts is not plain YAML: intrinsic functions may be
//! written as short-form node tags (`!Ref`, `!GetAtt`, `!Sub`, ...). A plain
//! YAML-to-JSON deserialize rejects those outright — the whole document fails,
//! not just the tagged node — so a template using the short forms parsed to
//! nothing at all and stacks came up empty (#2480).
//!
//! Everything that reads a template body (CloudFormation, Serverless
//! Application Repository, Config conformance packs) goes through here, so a
//! short form behaves exactly like its `{"Fn::Sub": ...}` long-form spelling.

use serde_json::{Map, Value as Json};
use serde_yaml::value::TaggedValue;
use serde_yaml::Value as Yaml;

/// Short-form tags that expand to `{"Fn::<Tag>": <arg>}`.
const FN_TAGS: &[&str] = &[
    "And",
    "Base64",
    "Cidr",
    "Equals",
    "FindInMap",
    "GetAZs",
    "GetAtt",
    "If",
    "ImportValue",
    "Join",
    "Length",
    "Not",
    "Or",
    "Select",
    "Split",
    "Sub",
    "ToJsonString",
    "Transform",
];

/// Short-form tags that expand to a bare `{"<Tag>": <arg>}` key. `Ref` and
/// `Condition` are the two CloudFormation spells without the `Fn::` prefix.
const BARE_TAGS: &[&str] = &["Ref", "Condition"];

/// Parse a CloudFormation template body (JSON or YAML, short-form tags
/// included) into a JSON value with every intrinsic in its long form.
///
/// A body starting with `{` is treated as JSON — YAML is a JSON superset, but
/// going through `serde_json` preserves exact numeric precision for the
/// programmatically-generated templates that dominate that case.
pub fn parse_template_body(body: &str) -> Result<Json, String> {
    if body.trim_start().starts_with('{') {
        return serde_json::from_str(body).map_err(|e| format!("Invalid JSON template: {e}"));
    }
    let yaml: Yaml =
        serde_yaml::from_str(body).map_err(|e| format!("Invalid YAML template: {e}"))?;
    Ok(yaml_to_json(yaml))
}

/// Convenience wrapper for callers that only want a template-shaped object and
/// treat anything else (a placeholder scalar, a parse failure) as absent.
pub fn parse_template_object(body: &str) -> Option<Json> {
    parse_template_body(body).ok().filter(Json::is_object)
}

fn yaml_to_json(value: Yaml) -> Json {
    match value {
        Yaml::Null => Json::Null,
        Yaml::Bool(b) => Json::Bool(b),
        Yaml::Number(n) => number_to_json(&n),
        Yaml::String(s) => Json::String(s),
        Yaml::Sequence(seq) => Json::Array(seq.into_iter().map(yaml_to_json).collect()),
        Yaml::Mapping(map) => Json::Object(
            map.into_iter()
                .map(|(k, v)| (mapping_key(k), yaml_to_json(v)))
                .collect(),
        ),
        Yaml::Tagged(tagged) => tagged_to_json(*tagged),
    }
}

/// JSON object keys are strings; a YAML mapping key that isn't one (`1: foo`)
/// takes its JSON rendering, matching how the key would have been written in
/// the equivalent JSON template.
fn mapping_key(key: Yaml) -> String {
    match yaml_to_json(key) {
        Json::String(s) => s,
        other => other.to_string(),
    }
}

fn number_to_json(n: &serde_yaml::Number) -> Json {
    if let Some(i) = n.as_i64() {
        return Json::Number(i.into());
    }
    if let Some(u) = n.as_u64() {
        return Json::Number(u.into());
    }
    n.as_f64()
        .and_then(serde_json::Number::from_f64)
        .map_or(Json::Null, Json::Number)
}

fn tagged_to_json(tagged: TaggedValue) -> Json {
    let rendered = tagged.tag.to_string();
    let name = rendered.strip_prefix('!').unwrap_or(&rendered);
    let arg = yaml_to_json(tagged.value);
    let key = if BARE_TAGS.contains(&name) {
        name.to_string()
    } else if FN_TAGS.contains(&name) {
        format!("Fn::{name}")
    } else {
        // Not a CloudFormation intrinsic. Keeping the node's value is more
        // useful than failing the whole document over an unrecognised tag.
        return arg;
    };
    let mut obj = Map::new();
    obj.insert(key, arg);
    Json::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn short_form_ref_becomes_long_form() {
        let parsed = parse_template_body(
            r"
Resources:
  ReproBucket:
    Type: AWS::S3::Bucket
Outputs:
  BucketName:
    Value: !Ref ReproBucket
",
        )
        .expect("template parses");
        assert_eq!(
            parsed["Outputs"]["BucketName"]["Value"],
            json!({"Ref": "ReproBucket"})
        );
        assert_eq!(
            parsed["Resources"]["ReproBucket"]["Type"],
            "AWS::S3::Bucket"
        );
    }

    #[test]
    fn every_intrinsic_short_form_expands() {
        let parsed = parse_template_body(
            r#"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Sub: !Sub "${AWS::StackName}-bucket"
      GetAtt: !GetAtt Other.Arn
      GetAttList: !GetAtt [Other, Arn]
      Join: !Join ["-", [a, b]]
      Select: !Select [0, [a, b]]
      Split: !Split [",", "a,b"]
      Base64: !Base64 hello
      FindInMap: !FindInMap [Map, Key, Value]
      GetAZs: !GetAZs us-east-1
      ImportValue: !ImportValue OtherStackExport
      Cidr: !Cidr ["10.0.0.0/16", 6, 5]
      Length: !Length [a, b]
      ToJsonString: !ToJsonString {a: b}
      Transform: !Transform {Name: Macro}
"#,
        )
        .expect("template parses");
        let props = &parsed["Resources"]["Thing"]["Properties"];
        assert_eq!(props["Sub"], json!({"Fn::Sub": "${AWS::StackName}-bucket"}));
        assert_eq!(props["GetAtt"], json!({"Fn::GetAtt": "Other.Arn"}));
        assert_eq!(props["GetAttList"], json!({"Fn::GetAtt": ["Other", "Arn"]}));
        assert_eq!(props["Join"], json!({"Fn::Join": ["-", ["a", "b"]]}));
        assert_eq!(props["Select"], json!({"Fn::Select": [0, ["a", "b"]]}));
        assert_eq!(props["Split"], json!({"Fn::Split": [",", "a,b"]}));
        assert_eq!(props["Base64"], json!({"Fn::Base64": "hello"}));
        assert_eq!(
            props["FindInMap"],
            json!({"Fn::FindInMap": ["Map", "Key", "Value"]})
        );
        assert_eq!(props["GetAZs"], json!({"Fn::GetAZs": "us-east-1"}));
        assert_eq!(
            props["ImportValue"],
            json!({"Fn::ImportValue": "OtherStackExport"})
        );
        assert_eq!(props["Cidr"], json!({"Fn::Cidr": ["10.0.0.0/16", 6, 5]}));
        assert_eq!(props["Length"], json!({"Fn::Length": ["a", "b"]}));
        assert_eq!(
            props["ToJsonString"],
            json!({"Fn::ToJsonString": {"a": "b"}})
        );
        assert_eq!(
            props["Transform"],
            json!({"Fn::Transform": {"Name": "Macro"}})
        );
    }

    #[test]
    fn condition_short_forms_expand() {
        let parsed = parse_template_body(
            r#"
Conditions:
  IsProd: !Equals [!Ref Env, prod]
  NotProd: !Not [!Condition IsProd]
  Either: !Or [!Condition IsProd, !Condition NotProd]
  Both: !And [!Condition IsProd, !Condition NotProd]
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Name: !If [IsProd, prod-bucket, dev-bucket]
"#,
        )
        .expect("template parses");
        assert_eq!(
            parsed["Conditions"]["IsProd"],
            json!({"Fn::Equals": [{"Ref": "Env"}, "prod"]})
        );
        assert_eq!(
            parsed["Conditions"]["NotProd"],
            json!({"Fn::Not": [{"Condition": "IsProd"}]})
        );
        assert_eq!(
            parsed["Conditions"]["Either"],
            json!({"Fn::Or": [{"Condition": "IsProd"}, {"Condition": "NotProd"}]})
        );
        assert_eq!(
            parsed["Conditions"]["Both"],
            json!({"Fn::And": [{"Condition": "IsProd"}, {"Condition": "NotProd"}]})
        );
        assert_eq!(
            parsed["Resources"]["Thing"]["Properties"]["Name"],
            json!({"Fn::If": ["IsProd", "prod-bucket", "dev-bucket"]})
        );
    }

    #[test]
    fn nested_short_forms_resolve_inside_out() {
        let parsed = parse_template_body(
            r#"
Resources:
  Thing:
    Type: AWS::SQS::Queue
    Properties:
      Name: !Join ["-", [!Ref Prefix, !GetAtt Other.Arn]]
"#,
        )
        .expect("template parses");
        assert_eq!(
            parsed["Resources"]["Thing"]["Properties"]["Name"],
            json!({"Fn::Join": ["-", [{"Ref": "Prefix"}, {"Fn::GetAtt": "Other.Arn"}]]})
        );
    }

    #[test]
    fn long_form_yaml_is_unchanged() {
        let parsed = parse_template_body(
            r#"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Name:
        Fn::Sub: "${AWS::StackName}-bucket"
      Owner:
        Ref: OwnerParam
"#,
        )
        .expect("template parses");
        let props = &parsed["Resources"]["Thing"]["Properties"];
        assert_eq!(
            props["Name"],
            json!({"Fn::Sub": "${AWS::StackName}-bucket"})
        );
        assert_eq!(props["Owner"], json!({"Ref": "OwnerParam"}));
    }

    #[test]
    fn json_templates_still_parse() {
        let parsed = parse_template_body(
            r#"{"Resources": {"Thing": {"Type": "AWS::S3::Bucket", "Properties": {"N": 1.5}}}}"#,
        )
        .expect("template parses");
        assert_eq!(parsed["Resources"]["Thing"]["Properties"]["N"], json!(1.5));
    }

    #[test]
    fn unknown_tags_keep_their_value() {
        let parsed = parse_template_body(
            r"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Custom: !SomethingElse hello
",
        )
        .expect("template parses");
        assert_eq!(
            parsed["Resources"]["Thing"]["Properties"]["Custom"],
            json!("hello")
        );
    }

    #[test]
    fn placeholder_bodies_are_not_objects() {
        assert!(parse_template_object("test").is_none());
        assert_eq!(parse_template_body("test").expect("scalar parses"), "test");
    }

    #[test]
    fn malformed_yaml_reports_an_error() {
        let err = parse_template_body("Resources:\n  - [unbalanced\n").expect_err("must fail");
        assert!(err.starts_with("Invalid YAML template:"), "{err}");
    }
}
