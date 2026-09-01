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

/// Short-form tags that expand to `{"Fn::<Tag>": <arg>}`. Covers the template
/// intrinsics, the condition functions, and the `Rules`-section functions —
/// CloudFormation defines no others, which is what lets an unrecognised tag be
/// treated as an error rather than quietly unwrapped.
const FN_TAGS: &[&str] = &[
    "And",
    "Base64",
    "Cidr",
    "Contains",
    "EachMemberEquals",
    "EachMemberIn",
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
    "RefAll",
    "Select",
    "Split",
    "Sub",
    "ToJsonString",
    "Transform",
    "ValueOf",
    "ValueOfAll",
];

/// Short-form tags that expand to a bare `{"<Tag>": <arg>}` key. `Ref` and
/// `Condition` are the two CloudFormation spells without the `Fn::` prefix.
const BARE_TAGS: &[&str] = &["Ref", "Condition"];

/// Parse a CloudFormation template body (JSON or YAML, short-form tags
/// included) into a JSON value with every intrinsic in its long form.
///
/// A body starting with `{` is tried as JSON first — YAML is a JSON superset,
/// but going through `serde_json` preserves exact numeric precision for the
/// programmatically-generated templates that dominate that case. It still
/// falls back to YAML on a JSON parse error, because YAML *flow* style also
/// opens with `{` (`{Resources: {A: {Type: X}}}` is valid YAML and invalid
/// JSON — unquoted keys), and committing to JSON on the first character would
/// silently drop such a template.
pub fn parse_template_body(body: &str) -> Result<Json, String> {
    if body.trim_start().starts_with('{') {
        return match serde_json::from_str(body) {
            Ok(value) => Ok(value),
            // Report the JSON error, not the YAML one: for a `{`-leading body
            // that is the diagnostic the author needs.
            Err(json_err) => {
                parse_yaml(body).map_err(|_| format!("Invalid JSON template: {json_err}"))
            }
        };
    }
    parse_yaml(body)
}

fn parse_yaml(body: &str) -> Result<Json, String> {
    let yaml: Yaml =
        serde_yaml::from_str(body).map_err(|e| format!("Invalid YAML template: {e}"))?;
    yaml_to_json(yaml).map_err(|e| format!("Invalid YAML template: {e}"))
}

/// Whether a body is meant to be a CloudFormation template *document*, as
/// opposed to a placeholder scalar (the conformance probe sends `"test"`-style
/// strings for `TemplateBody`).
///
/// A body that parses is judged on whether it carries a `Resources` section.
/// One that does NOT parse still has to be classified — and it is exactly the
/// interesting case, since a syntax error (a stray tab, a bad indent, an
/// unbalanced bracket) is the most common way a real template breaks. Re-using
/// the parser here would answer "not a template" for every syntax error and
/// send it down the lenient degrade-to-empty path, which is the silent no-op
/// #2480 is about. So fall back to a shape check on the raw text.
pub fn is_template_document(body: &str) -> bool {
    if let Ok(value) = parse_template_body(body) {
        // Presence, not shape: `Resources:` holding a sequence instead of a
        // mapping is a common YAML slip that the template parser rejects. It
        // is still unmistakably someone's template, so it must fail loudly
        // rather than degrade to an empty stack.
        return value.get("Resources").is_some();
    }
    looks_like_template_text(body)
}

/// Text-level shape check for a body that would not parse: does it *look* like
/// someone's template? Keyed on a top-level `Resources` section, the one
/// section CloudFormation requires.
fn looks_like_template_text(body: &str) -> bool {
    if body.trim_start().starts_with('{') {
        // Both spellings: JSON quotes the key, YAML flow style does not.
        return body.contains("\"Resources\"") || body.contains("Resources:");
    }
    // A top-level YAML key sits at column zero. It may be quoted, and may
    // carry whitespace before its colon — both legal, and both emitted by
    // real generators.
    body.lines().any(declares_resources)
}

fn declares_resources(line: &str) -> bool {
    let rest = line
        .strip_prefix('"')
        .or_else(|| line.strip_prefix('\''))
        .unwrap_or(line);
    let Some(rest) = rest.strip_prefix("Resources") else {
        return false;
    };
    let rest = rest
        .strip_prefix('"')
        .or_else(|| rest.strip_prefix('\''))
        .unwrap_or(rest);
    let rest = rest.trim_start();
    rest.is_empty() || rest.starts_with(':')
}

/// Convenience wrapper for callers that only want a template-shaped object and
/// treat anything else (a placeholder scalar, a parse failure) as absent.
pub fn parse_template_object(body: &str) -> Option<Json> {
    parse_template_body(body).ok().filter(Json::is_object)
}

fn yaml_to_json(value: Yaml) -> Result<Json, String> {
    Ok(match value {
        Yaml::Null => Json::Null,
        Yaml::Bool(b) => Json::Bool(b),
        Yaml::Number(n) => number_to_json(&n)?,
        Yaml::String(s) => Json::String(s),
        Yaml::Sequence(seq) => Json::Array(
            seq.into_iter()
                .map(yaml_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Yaml::Mapping(map) => {
            let mut obj = Map::new();
            for (k, v) in map {
                obj.insert(mapping_key(k)?, yaml_to_json(v)?);
            }
            Json::Object(obj)
        }
        Yaml::Tagged(tagged) => tagged_to_json(*tagged)?,
    })
}

/// JSON object keys are strings; a YAML mapping key that isn't one (`1: foo`)
/// takes its JSON rendering, matching how the key would have been written in
/// the equivalent JSON template.
fn mapping_key(key: Yaml) -> Result<String, String> {
    Ok(match yaml_to_json(key)? {
        Json::String(s) => s,
        other => other.to_string(),
    })
}

fn number_to_json(n: &serde_yaml::Number) -> Result<Json, String> {
    if let Some(i) = n.as_i64() {
        return Ok(Json::Number(i.into()));
    }
    if let Some(u) = n.as_u64() {
        return Ok(Json::Number(u.into()));
    }
    // YAML has `.nan` / `.inf`; JSON has no representation for either, and no
    // CloudFormation property legitimately holds one. Surfacing the bad value
    // beats silently rewriting it to `null`, which reads downstream as a
    // deliberately-absent property.
    n.as_f64()
        .and_then(serde_json::Number::from_f64)
        .map(Json::Number)
        .ok_or_else(|| format!("number {n} has no JSON representation"))
}

fn tagged_to_json(tagged: TaggedValue) -> Result<Json, String> {
    let rendered = tagged.tag.to_string();
    let name = rendered.strip_prefix('!').unwrap_or(&rendered);
    let arg = yaml_to_json(tagged.value)?;
    let key = if BARE_TAGS.contains(&name) {
        name.to_string()
    } else if FN_TAGS.contains(&name) {
        format!("Fn::{name}")
    } else if name.starts_with('!') {
        // A YAML *standard* tag (`!!str`, `!!int`, ...) — the second `!`
        // survives the strip above. Not a CloudFormation intrinsic, and the
        // value it decorates is what matters.
        return Ok(arg);
    } else {
        // An unrecognised single-`!` tag. CloudFormation defines a closed set
        // of short forms, so this is a typo (`!GettAtt`) or an unsupported
        // function. Unwrapping it to its argument would turn `!GettAtt
        // Topic.Arn` into the literal string "Topic.Arn" and provision a
        // wrong-but-plausible value under a CREATE_COMPLETE stack — the same
        // silent-wrong-value class this module exists to close.
        return Err(format!("unknown intrinsic tag !{name}"));
    };
    let mut obj = Map::new();
    obj.insert(key, arg);
    Ok(Json::Object(obj))
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
    fn unknown_tags_are_an_error() {
        // This originally asserted the tag unwrapped to its value. That is
        // precisely the silent-wrong-value behaviour the module exists to
        // prevent: the property would provision as the string "hello" under a
        // CREATE_COMPLETE stack, with nothing reported.
        let err = parse_template_body(
            r"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Custom: !SomethingElse hello
",
        )
        .expect_err("an unrecognised intrinsic must be reported");
        assert!(
            err.contains("unknown intrinsic tag !SomethingElse"),
            "{err}"
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

    #[test]
    fn yaml_flow_style_body_still_parses() {
        // Valid YAML, invalid JSON (unquoted keys) despite the leading `{`.
        // Committing to JSON on the first character would drop it silently.
        let parsed =
            parse_template_body("{Resources: {A: {Type: AWS::SQS::Queue}}}").expect("parses");
        assert_eq!(parsed["Resources"]["A"]["Type"], "AWS::SQS::Queue");
        assert!(parse_template_object("{Resources: {A: {Type: AWS::SQS::Queue}}}").is_some());
    }

    #[test]
    fn json_body_that_is_neither_reports_the_json_error() {
        let err = parse_template_body("{\"Resources\": [oops").expect_err("must fail");
        assert!(err.starts_with("Invalid JSON template:"), "{err}");
    }

    #[test]
    fn syntax_errors_are_still_recognised_as_template_documents() {
        // The #2480 case: a template that does not parse must still be
        // classified as a template, so the caller can fail loudly instead of
        // degrading to an empty stack.
        let tab_indent = "Resources:\n\tBad: indented with a tab\n";
        assert!(parse_template_body(tab_indent).is_err());
        assert!(is_template_document(tab_indent));

        let unbalanced = "Resources:\n  Queue:\n    Type: [AWS::SQS::Queue\n";
        assert!(parse_template_body(unbalanced).is_err());
        assert!(is_template_document(unbalanced));

        // Unbalanced in both dialects, so neither the JSON nor the YAML pass
        // can rescue it.
        let bad_json = "{\"Resources\": [oops";
        assert!(parse_template_body(bad_json).is_err());
        assert!(is_template_document(bad_json));
    }

    #[test]
    fn unknown_tags_are_rejected_not_unwrapped() {
        // A one-character typo used to unwrap to the literal string
        // "Topic.Arn" and provision a wrong-but-plausible value under a
        // CREATE_COMPLETE stack.
        let err = parse_template_body(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      V: !GettAtt Topic.Arn\n",
        )
        .expect_err("a misspelled intrinsic must not be silently unwrapped");
        assert!(err.contains("unknown intrinsic tag !GettAtt"), "{err}");
    }

    #[test]
    fn rules_section_short_forms_expand() {
        let parsed = parse_template_body(
            r#"
Rules:
  R:
    Assertions:
      - Assert: !Contains [[a, b], !Ref Thing]
      - Assert: !EachMemberEquals [[a], a]
      - Assert: !EachMemberIn [[a], [a, b]]
      - Assert: !ValueOf [Param, Tags]
      - Assert: !ValueOfAll ["AWS::EC2::Subnet::Id", VpcId]
      - Assert: !RefAll "AWS::EC2::VPC::Id"
Resources:
  Q:
    Type: AWS::SQS::Queue
"#,
        )
        .expect("Rules-section short forms are CloudFormation intrinsics");
        let asserts = &parsed["Rules"]["R"]["Assertions"];
        assert_eq!(
            asserts[0]["Assert"],
            json!({"Fn::Contains": [["a", "b"], {"Ref": "Thing"}]})
        );
        assert_eq!(
            asserts[1]["Assert"],
            json!({"Fn::EachMemberEquals": [["a"], "a"]})
        );
        assert_eq!(
            asserts[2]["Assert"],
            json!({"Fn::EachMemberIn": [["a"], ["a", "b"]]})
        );
        assert_eq!(
            asserts[3]["Assert"],
            json!({"Fn::ValueOf": ["Param", "Tags"]})
        );
        assert_eq!(
            asserts[5]["Assert"],
            json!({"Fn::RefAll": "AWS::EC2::VPC::Id"})
        );
    }

    #[test]
    fn yaml_standard_tags_still_pass_through() {
        // `!!str` and friends are YAML's own tags, not CloudFormation's, and
        // must not trip the unknown-intrinsic check.
        let parsed = parse_template_body(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      A: !!str 5\n      B: !!int \"7\"\n",
        )
        .expect("standard YAML tags are not CloudFormation intrinsics");
        let props = &parsed["Resources"]["Q"]["Properties"];
        assert_eq!(props["A"], json!("5"));
        assert_eq!(props["B"], json!(7));
    }

    #[test]
    fn quoted_or_spaced_resources_key_is_recognised() {
        // Legal YAML spellings of the top-level key, each paired with a
        // syntax error elsewhere so classification runs on the raw text.
        for body in [
            "\"Resources\":\n\tQ:\n\t\tType: AWS::SQS::Queue\n",
            "'Resources':\n\tQ:\n\t\tType: AWS::SQS::Queue\n",
            "Resources :\n\tQ:\n\t\tType: AWS::SQS::Queue\n",
        ] {
            assert!(
                parse_template_body(body).is_err(),
                "{body:?} should not parse"
            );
            assert!(
                is_template_document(body),
                "{body:?} must be recognised as a template"
            );
        }
    }

    #[test]
    fn wrong_shaped_resources_section_is_still_a_template_document() {
        // `Resources` as a sequence instead of a mapping is a common YAML
        // slip. The template parser rejects it, so it has to be classified as
        // a template or it degrades to a silent empty stack.
        let seq = "Resources:\n  - Type: AWS::SQS::Queue\n";
        assert!(parse_template_body(seq).is_ok(), "parses as YAML");
        assert!(is_template_document(seq));

        // Unparseable flow-style YAML: the key is unquoted, so a check for
        // the JSON spelling alone would miss it.
        let broken_flow = "{Resources: {A: {Type: AWS::SQS::Queue}";
        assert!(parse_template_body(broken_flow).is_err());
        assert!(is_template_document(broken_flow));
    }

    #[test]
    fn non_finite_numbers_are_reported_not_nulled() {
        let err = parse_template_body("Resources:\n  Q:\n    Timeout: .nan\n")
            .expect_err("NaN has no JSON representation");
        assert!(err.contains("no JSON representation"), "{err}");
        // Still classified as a template, so the caller fails loudly.
        assert!(is_template_document(
            "Resources:\n  Q:\n    Timeout: .nan\n"
        ));
    }

    #[test]
    fn placeholders_are_not_template_documents() {
        // The conformance probe's synthetic TemplateBody values must keep the
        // lenient path — ValidationError is not in CreateStack's Smithy errors.
        for placeholder in ["test", "t", "aaaaaaaa", "", "dGVzdA=="] {
            assert!(
                !is_template_document(placeholder),
                "{placeholder:?} must not be treated as a template"
            );
        }
        // A parseable document with no Resources section isn't one either.
        assert!(!is_template_document("Description: just a description\n"));
    }

    #[test]
    fn well_formed_templates_are_template_documents() {
        assert!(is_template_document(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n"
        ));
        assert!(is_template_document(
            r#"{"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}}"#
        ));
    }
}
