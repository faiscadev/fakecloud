//! Template introspection shared by `ValidateTemplate` and
//! `GetTemplateSummary`.
//!
//! Both operations answer questions about a template without provisioning
//! anything: what parameters does it declare, what capabilities does it need,
//! what resource types does it contain. They previously returned a fixed
//! `<Description>Validated</Description>` with empty parameter and capability
//! lists, so `aws cloudformation validate-template` reported success on a
//! template that would then provision nothing -- the first command in the
//! #2480 report, giving a false green light before the real failure.
//!
//! Two levels of leniency, which the service layer relies on:
//!
//! - A body that is not a template at all -- the conformance probe sends
//!   `TemplateBody="test"` -- yields the empty summary. Neither operation
//!   declares `ValidationError` in its Smithy `errors` list, so it must not
//!   become one.
//! - A body that IS unmistakably a template but is structurally invalid gets a
//!   problem back from [`structural_error`], which the handlers turn into a
//!   `ValidationError`. That path is gated on
//!   `cfn_template::is_template_document`, so the probe's inputs never reach
//!   it.
//!
//! Nothing here errors on its own; `summarize` always returns a summary and
//! `structural_error` always returns an `Option`.

use fakecloud_aws::xml::xml_escape;
use serde_json::Value;

/// One declared template parameter, with the constraints AWS reports.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct SummaryParameter {
    pub key: String,
    pub default_value: Option<String>,
    pub parameter_type: String,
    pub no_echo: bool,
    pub description: Option<String>,
    pub allowed_values: Vec<String>,
}

/// What both operations report about a template.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct TemplateSummary {
    pub description: Option<String>,
    pub parameters: Vec<SummaryParameter>,
    pub capabilities: Vec<String>,
    pub capabilities_reason: Option<String>,
    pub resource_types: Vec<String>,
    pub declared_transforms: Vec<String>,
    pub version: Option<String>,
    pub metadata: Option<String>,
}

/// Whether a resource type is one of AWS's own IAM types.
///
/// An exact prefix, not a substring: a third-party or custom type whose name
/// merely contains `::IAM::` (`MyOrg::IAM::Thing`, `Custom::IAM::Wrapper`)
/// does not require an IAM capability, and matching it would report a
/// capability the caller does not need -- and name it in the reason.
fn is_aws_iam_type(resource_type: &str) -> bool {
    resource_type.starts_with("AWS::IAM::")
}

/// Explicit-name properties. A template that names an IAM resource needs
/// `CAPABILITY_NAMED_IAM` rather than plain `CAPABILITY_IAM`, because the name
/// can collide with an existing principal.
const IAM_NAME_PROPERTIES: &[&str] = &[
    "RoleName",
    "UserName",
    "GroupName",
    "PolicyName",
    "ManagedPolicyName",
    "InstanceProfileName",
    "ServerCertificateName",
];

/// The structural problem with a template, if any -- the level
/// `ValidateTemplate` and `GetTemplateSummary` actually check.
///
/// Deliberately NOT the full `template::parse_template` that CreateStack runs.
/// That resolves parameter *values*, so a template whose structure depends on
/// a parameter the caller never supplies (`!FindInMap [Config, !Ref Env, Size]`
/// with no default for `Env`, or `Fn::ForEach` over a `Ref`) would be reported
/// as invalid. Real `validate-template` never resolves values and accepts
/// those templates -- and CDK and `sam deploy` call `GetTemplateSummary`
/// during deploy, so rejecting them would break the deploy before it starts.
///
/// What it does check is what AWS checks without values: the body parses in
/// its dialect, there is a `Resources` mapping with at least one entry, and
/// every resource carries a `Type`.
pub(crate) fn structural_error(body: &str) -> Option<String> {
    let value = match fakecloud_core::cfn_template::parse_template_body(body) {
        Ok(value) => value,
        Err(err) => return Some(err),
    };
    let Some(obj) = value.as_object() else {
        return Some("Template format error: unsupported structure.".to_string());
    };
    let Some(resources) = obj.get("Resources") else {
        return Some(
            "Template format error: At least one Resources member must be defined.".to_string(),
        );
    };
    let Some(resources) = resources.as_object() else {
        return Some("Template format error: [/Resources] must be an object.".to_string());
    };
    if resources.is_empty() {
        return Some(
            "Template format error: At least one Resources member must be defined.".to_string(),
        );
    }
    for (logical_id, resource) in resources {
        let has_type = resource
            .get("Type")
            .and_then(Value::as_str)
            .is_some_and(|t| !t.is_empty());
        if !has_type {
            return Some(format!(
                "Template format error: Every Resources object must contain a Type member. \
                 Resource {logical_id} does not."
            ));
        }
    }
    None
}

/// Build the summary. A body that does not parse, or parses to something that
/// is not a mapping, yields the empty summary.
pub(crate) fn summarize(template_body: &str) -> TemplateSummary {
    let Some(value) = fakecloud_core::cfn_template::parse_template_object(template_body) else {
        return TemplateSummary::default();
    };

    let description = value
        .get("Description")
        .and_then(Value::as_str)
        .map(str::to_string);
    let version = value
        .get("AWSTemplateFormatVersion")
        .and_then(Value::as_str)
        .map(str::to_string);
    // Same reasoning as a null parameter default: a blank `Metadata:` parses
    // to Null, and reporting the literal "null" invents content the template
    // never carried.
    let metadata = value
        .get("Metadata")
        .filter(|m| !m.is_null())
        .map(ToString::to_string);

    // Computed once and shared: `capabilities` and `capabilities_reason` both
    // need the IAM types and the transforms, and deriving them separately in
    // three places both re-walked the template and gave the three copies room
    // to drift apart.
    let transforms = declared_transforms(&value);
    let iam = iam_resource_types(&value);
    let named_iam = has_named_iam(&value);

    TemplateSummary {
        description,
        parameters: parameters(&value),
        capabilities: capabilities(&iam, &transforms, named_iam),
        capabilities_reason: capabilities_reason(&iam, &transforms),
        resource_types: resource_types(&value),
        declared_transforms: transforms,
        version,
        metadata,
    }
}

fn parameters(value: &Value) -> Vec<SummaryParameter> {
    let Some(declared) = value.get("Parameters").and_then(Value::as_object) else {
        return Vec::new();
    };
    declared
        .iter()
        .map(|(key, spec)| SummaryParameter {
            key: key.clone(),
            // A non-string default (a Number parameter's `Default: 3`) is
            // reported in its JSON spelling, which is how it arrives on the
            // wire everywhere else in this crate.
            default_value: spec.get("Default").and_then(scalar_to_string),
            parameter_type: spec
                .get("Type")
                .and_then(Value::as_str)
                .unwrap_or("String")
                .to_string(),
            no_echo: spec.get("NoEcho").map(truthy).unwrap_or(false),
            description: spec
                .get("Description")
                .and_then(Value::as_str)
                .map(str::to_string),
            allowed_values: spec
                .get("AllowedValues")
                .and_then(Value::as_array)
                .map(|vals| vals.iter().filter_map(scalar_to_string).collect())
                .unwrap_or_default(),
        })
        .collect()
}

/// `NoEcho` is a boolean in JSON templates and frequently the string `"true"`
/// in YAML ones.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::String(s) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// A scalar in its wire spelling. `Null` is absent rather than the literal
/// `"null"`: a YAML `Default:` with nothing after it parses to `Null`, and
/// reporting that as the default invents a value the template never declared.
fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn resource_types(value: &Value) -> Vec<String> {
    let Some(resources) = value.get("Resources").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut types: Vec<String> = resources
        .values()
        .filter_map(|r| r.get("Type").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    types.sort();
    types.dedup();
    types
}

/// `Transform` takes a single name, a list, or the mapping form that carries
/// arguments (`{Name: AWS::Include, Parameters: {...}}`) -- including inside a
/// list. Missing the mapping form meant a template that declares one reported
/// no transforms and, through `capabilities`, omitted CAPABILITY_AUTO_EXPAND:
/// `validate-template` would say no capability was needed for a template
/// `create-stack` then rejects for lacking it.
fn declared_transforms(value: &Value) -> Vec<String> {
    fn name_of(value: &Value) -> Option<String> {
        match value {
            Value::String(s) => Some(s.clone()),
            Value::Object(map) => map.get("Name").and_then(Value::as_str).map(str::to_string),
            _ => None,
        }
    }
    match value.get("Transform") {
        Some(Value::Array(items)) => items.iter().filter_map(name_of).collect(),
        Some(other) => name_of(other).into_iter().collect(),
        None => Vec::new(),
    }
}

/// The IAM resource types a template declares, which drive both the capability
/// list and the reason string.
fn iam_resource_types(value: &Value) -> Vec<String> {
    let Some(resources) = value.get("Resources").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut types: Vec<String> = resources
        .values()
        .filter_map(|r| r.get("Type").and_then(Value::as_str))
        .filter(|t| is_aws_iam_type(t))
        .map(str::to_string)
        .collect();
    types.sort();
    types.dedup();
    types
}

fn has_named_iam(value: &Value) -> bool {
    let Some(resources) = value.get("Resources").and_then(Value::as_object) else {
        return false;
    };
    resources.values().any(|r| {
        let is_iam = r
            .get("Type")
            .and_then(Value::as_str)
            .is_some_and(is_aws_iam_type);
        is_iam
            && r.get("Properties")
                .is_some_and(|p| IAM_NAME_PROPERTIES.iter().any(|name| p.get(name).is_some()))
    })
}

fn capabilities(iam: &[String], transforms: &[String], named_iam: bool) -> Vec<String> {
    let mut caps = Vec::new();
    if !iam.is_empty() {
        // The two IAM capabilities are exclusive: naming a resource implies
        // the broader grant, and AWS reports only the narrower-scoped name.
        if named_iam {
            caps.push("CAPABILITY_NAMED_IAM".to_string());
        } else {
            caps.push("CAPABILITY_IAM".to_string());
        }
    }
    // A transform (SAM, `AWS::Include`, a macro) rewrites the template before
    // provisioning, so the caller has to accept expansion.
    if !transforms.is_empty() {
        caps.push("CAPABILITY_AUTO_EXPAND".to_string());
    }
    caps
}

/// AWS explains WHICH resources forced the capability, which is the part that
/// makes the error actionable when a deploy is rejected for missing it.
fn capabilities_reason(iam: &[String], transforms: &[String]) -> Option<String> {
    // Transforms count too. Reporting CAPABILITY_AUTO_EXPAND with no reason
    // left every SAM or macro template explaining a rejected deploy with an
    // empty string.
    let mut forced: Vec<String> = iam.to_vec();
    forced.extend_from_slice(transforms);
    if forced.is_empty() {
        return None;
    }
    Some(format!(
        "The following resource(s) require capabilities: [{}]",
        forced.join(", ")
    ))
}

fn members_xml(indent: &str, tag: &str, values: &[String]) -> String {
    if values.is_empty() {
        return format!("{indent}<{tag}/>");
    }
    let members = values
        .iter()
        .map(|v| format!("{indent}  <member>{}</member>", xml_escape(v)))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{indent}<{tag}>\n{members}\n{indent}</{tag}>")
}

fn optional_element(indent: &str, tag: &str, value: Option<&str>) -> String {
    value.map_or_else(String::new, |v| {
        format!("\n{indent}<{tag}>{}</{tag}>", xml_escape(v))
    })
}

/// `ValidateTemplate` reports `TemplateParameter` members: key, default,
/// NoEcho and description, but no type or constraints.
pub(crate) fn validate_template_xml(summary: &TemplateSummary) -> String {
    let params = if summary.parameters.is_empty() {
        "    <Parameters/>".to_string()
    } else {
        let members = summary
            .parameters
            .iter()
            .map(|p| {
                format!(
                    "      <member>\n        <ParameterKey>{}</ParameterKey>{}\n        <NoEcho>{}</NoEcho>{}\n      </member>",
                    xml_escape(&p.key),
                    optional_element("        ", "DefaultValue", p.default_value.as_deref()),
                    p.no_echo,
                    optional_element("        ", "Description", p.description.as_deref()),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("    <Parameters>\n{members}\n    </Parameters>")
    };

    format!(
        "{params}{}\n{}{}\n{}",
        optional_element("    ", "Description", summary.description.as_deref()),
        members_xml("    ", "Capabilities", &summary.capabilities),
        optional_element(
            "    ",
            "CapabilitiesReason",
            summary.capabilities_reason.as_deref()
        ),
        members_xml("    ", "DeclaredTransforms", &summary.declared_transforms),
    )
}

/// `GetTemplateSummary` reports `ParameterDeclaration` members, which add the
/// parameter type and its `AllowedValues` constraints, plus the template's
/// resource types, version and metadata.
pub(crate) fn get_template_summary_xml(summary: &TemplateSummary) -> String {
    let params = if summary.parameters.is_empty() {
        "    <Parameters/>".to_string()
    } else {
        let members = summary
            .parameters
            .iter()
            .map(|p| {
                let constraints = if p.allowed_values.is_empty() {
                    String::new()
                } else {
                    format!(
                        "\n        <ParameterConstraints>\n{}\n        </ParameterConstraints>",
                        members_xml("          ", "AllowedValues", &p.allowed_values)
                    )
                };
                format!(
                    "      <member>\n        <ParameterKey>{}</ParameterKey>{}\n        <ParameterType>{}</ParameterType>\n        <NoEcho>{}</NoEcho>{}{}\n      </member>",
                    xml_escape(&p.key),
                    optional_element("        ", "DefaultValue", p.default_value.as_deref()),
                    xml_escape(&p.parameter_type),
                    p.no_echo,
                    optional_element("        ", "Description", p.description.as_deref()),
                    constraints,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("    <Parameters>\n{members}\n    </Parameters>")
    };

    format!(
        "{params}{}\n{}{}\n{}\n{}{}{}",
        optional_element("    ", "Description", summary.description.as_deref()),
        members_xml("    ", "Capabilities", &summary.capabilities),
        optional_element(
            "    ",
            "CapabilitiesReason",
            summary.capabilities_reason.as_deref()
        ),
        members_xml("    ", "ResourceTypes", &summary.resource_types),
        members_xml("    ", "DeclaredTransforms", &summary.declared_transforms),
        optional_element("    ", "Version", summary.version.as_deref()),
        optional_element("    ", "Metadata", summary.metadata.as_deref()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const IAM_TEMPLATE: &str = r#"
AWSTemplateFormatVersion: '2010-09-09'
Description: A template that needs IAM
Metadata:
  Build: 42
Parameters:
  Stage:
    Type: String
    Default: dev
    Description: Deployment stage
    AllowedValues: [dev, prod]
  Secret:
    Type: String
    NoEcho: true
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument: {}
  Q:
    Type: AWS::SQS::Queue
"#;

    #[test]
    fn summarizes_parameters_capabilities_and_types() {
        let s = summarize(IAM_TEMPLATE);
        assert_eq!(s.description.as_deref(), Some("A template that needs IAM"));
        assert_eq!(s.version.as_deref(), Some("2010-09-09"));
        assert!(s.metadata.is_some());
        assert_eq!(s.resource_types, ["AWS::IAM::Role", "AWS::SQS::Queue"]);
        // An unnamed IAM resource needs only the broad capability.
        assert_eq!(s.capabilities, ["CAPABILITY_IAM"]);
        assert_eq!(
            s.capabilities_reason.as_deref(),
            Some("The following resource(s) require capabilities: [AWS::IAM::Role]")
        );

        let stage = s.parameters.iter().find(|p| p.key == "Stage").unwrap();
        assert_eq!(stage.parameter_type, "String");
        assert_eq!(stage.default_value.as_deref(), Some("dev"));
        assert_eq!(stage.description.as_deref(), Some("Deployment stage"));
        assert_eq!(stage.allowed_values, ["dev", "prod"]);
        assert!(!stage.no_echo);

        let secret = s.parameters.iter().find(|p| p.key == "Secret").unwrap();
        assert!(secret.no_echo, "NoEcho must be reported");
        assert!(secret.default_value.is_none());
    }

    #[test]
    fn naming_an_iam_resource_requires_the_named_capability() {
        let named = r#"
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      RoleName: explicit-name
"#;
        assert_eq!(summarize(named).capabilities, ["CAPABILITY_NAMED_IAM"]);
    }

    #[test]
    fn a_transform_requires_auto_expand() {
        let sam = r#"
Transform: AWS::Serverless-2016-10-31
Resources:
  Fn:
    Type: AWS::Serverless::Function
"#;
        let s = summarize(sam);
        assert_eq!(s.capabilities, ["CAPABILITY_AUTO_EXPAND"]);
        assert_eq!(s.declared_transforms, ["AWS::Serverless-2016-10-31"]);

        // A list of transforms is also legal.
        let many = "Transform: [AWS::Include, MyMacro]\nResources: {}\n";
        assert_eq!(
            summarize(many).declared_transforms,
            ["AWS::Include", "MyMacro"]
        );
    }

    #[test]
    fn short_form_templates_summarize_too() {
        // The whole point of #2480: a `!Ref`-bearing template must parse here
        // as well, or validate-template reports an empty summary for a
        // perfectly good template.
        let s = summarize(
            "Parameters:\n  P:\n    Type: String\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: !Ref P\n",
        );
        assert_eq!(s.resource_types, ["AWS::SQS::Queue"]);
        assert_eq!(s.parameters.len(), 1);
    }

    #[test]
    fn the_transform_mapping_form_counts() {
        // `Transform: {Name: ..., Parameters: {...}}` is as real as the string
        // form. Missing it reported no transforms and, worse, omitted
        // CAPABILITY_AUTO_EXPAND -- telling the caller no capability was
        // needed for a template create-stack would then reject.
        let mapping = "Transform:\n  Name: AWS::Include\n  Parameters:\n    Location: s3://b/k\nResources: {}\n";
        let s = summarize(mapping);
        assert_eq!(s.declared_transforms, ["AWS::Include"]);
        assert_eq!(s.capabilities, ["CAPABILITY_AUTO_EXPAND"]);

        // Also inside a list, mixed with plain names.
        let mixed =
            "Transform:\n  - AWS::Serverless-2016-10-31\n  - Name: MyMacro\nResources: {}\n";
        assert_eq!(
            summarize(mixed).declared_transforms,
            ["AWS::Serverless-2016-10-31", "MyMacro"]
        );
    }

    #[test]
    fn a_null_default_is_absent_not_the_string_null() {
        // `Default:` with nothing after it parses to Null. Reporting "null"
        // would invent a default the template never declared.
        let s = summarize("Parameters:\n  P:\n    Type: String\n    Default:\nResources: {}\n");
        let p = &s.parameters[0];
        assert_eq!(p.default_value, None, "a null default must be absent");

        // A real default still reports, including a non-string one.
        let s = summarize("Parameters:\n  N:\n    Type: Number\n    Default: 3\nResources: {}\n");
        assert_eq!(s.parameters[0].default_value.as_deref(), Some("3"));
    }

    #[test]
    fn structural_check_does_not_resolve_parameter_values() {
        // The over-correction this guards against: running CreateStack's full
        // parse here rejected a template whose structure depends on a
        // parameter the caller never supplies. Real validate-template never
        // resolves values, and CDK / `sam deploy` call GetTemplateSummary
        // during deploy -- rejecting these would break the deploy up front.
        let unresolved_findinmap = r#"
Parameters:
  Env:
    Type: String
Mappings:
  Config:
    dev:
      Size: t2.micro
Resources:
  I:
    Type: AWS::EC2::Instance
    Properties:
      InstanceType: !FindInMap [Config, !Ref Env, Size]
"#;
        assert_eq!(structural_error(unresolved_findinmap), None);

        // A `Ref` to a parameter with no default is likewise fine.
        let unresolved_ref = "Parameters:\n  P:\n    Type: String\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      QueueName: !Ref P\n";
        assert_eq!(structural_error(unresolved_ref), None);
    }

    #[test]
    fn structural_check_catches_what_aws_catches() {
        // No Resources section at all.
        let err = structural_error("Parameters:\n  P:\n    Type: String\n").unwrap();
        assert!(err.contains("At least one Resources member"), "{err}");

        // Resources present but empty.
        let err = structural_error("Resources: {}\n").unwrap();
        assert!(err.contains("At least one Resources member"), "{err}");

        // A resource with no Type.
        let err = structural_error("Resources:\n  Q:\n    Properties: {}\n").unwrap();
        assert!(err.contains("must contain a Type member"), "{err}");
        assert!(
            err.contains('Q'),
            "the offending resource should be named: {err}"
        );

        // Resources is a sequence, not a mapping.
        let err = structural_error("Resources:\n  - Type: AWS::SQS::Queue\n").unwrap();
        assert!(err.contains("[/Resources]"), "{err}");

        // A dialect error still reports.
        let err = structural_error("Resources:\n\tQ:\n\t\tType: AWS::SQS::Queue\n").unwrap();
        assert!(err.contains("Invalid YAML template"), "{err}");

        // A good template has nothing to report.
        assert_eq!(
            structural_error("Resources:\n  Q:\n    Type: AWS::SQS::Queue\n"),
            None
        );
    }

    #[test]
    fn only_aws_iam_types_force_an_iam_capability() {
        // A third-party type whose name merely contains `::IAM::` is not an
        // AWS IAM resource; reporting a capability for it would name a
        // resource the caller does not need to acknowledge.
        let third_party =
            "Resources:\n  T:\n    Type: MyOrg::IAM::Thing\n    Properties:\n      RoleName: x\n";
        let s = summarize(third_party);
        assert!(s.capabilities.is_empty(), "{:?}", s.capabilities);
        assert_eq!(s.capabilities_reason, None);

        // A real one still does.
        let aws = "Resources:\n  R:\n    Type: AWS::IAM::Role\n";
        assert_eq!(summarize(aws).capabilities, ["CAPABILITY_IAM"]);
    }

    #[test]
    fn a_named_server_certificate_needs_the_named_capability() {
        let s = summarize(
            "Resources:\n  C:\n    Type: AWS::IAM::ServerCertificate\n    Properties:\n      ServerCertificateName: explicit\n",
        );
        assert_eq!(s.capabilities, ["CAPABILITY_NAMED_IAM"]);
    }

    #[test]
    fn a_transform_only_template_explains_its_capability() {
        // CAPABILITY_AUTO_EXPAND with no reason left every SAM or macro
        // template explaining a rejected deploy with an empty string.
        let s = summarize("Transform: AWS::Serverless-2016-10-31\nResources:\n  F:\n    Type: AWS::Serverless::Function\n");
        assert_eq!(s.capabilities, ["CAPABILITY_AUTO_EXPAND"]);
        let reason = s.capabilities_reason.expect("a reason must be given");
        assert!(reason.contains("AWS::Serverless-2016-10-31"), "{reason}");
    }

    #[test]
    fn blank_metadata_is_absent_not_the_string_null() {
        let s = summarize("Metadata:\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n");
        assert_eq!(
            s.metadata, None,
            "a blank Metadata must not report \"null\""
        );

        // Real metadata still reports.
        let s = summarize("Metadata:\n  Build: 42\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n");
        assert!(s.metadata.is_some_and(|m| m.contains("Build")));
    }

    #[test]
    fn placeholder_bodies_summarize_to_nothing() {
        // The conformance probe's inputs. Neither operation declares
        // ValidationError, so these must degrade rather than fail.
        for body in ["test", "", "   ", "[]", "not a template"] {
            let s = summarize(body);
            assert_eq!(s, TemplateSummary::default(), "{body:?}");
        }
    }

    #[test]
    fn empty_summary_renders_the_shapes_aws_declares() {
        let xml = validate_template_xml(&TemplateSummary::default());
        assert!(xml.contains("<Parameters/>"), "{xml}");
        assert!(xml.contains("<Capabilities/>"), "{xml}");
        assert!(!xml.contains("<CapabilitiesReason>"), "{xml}");

        let xml = get_template_summary_xml(&TemplateSummary::default());
        assert!(xml.contains("<Parameters/>"));
        assert!(xml.contains("<ResourceTypes/>"));
        assert!(xml.contains("<Capabilities/>"));
    }

    #[test]
    fn populated_summary_renders_each_field() {
        let s = summarize(IAM_TEMPLATE);

        let xml = validate_template_xml(&s);
        assert!(xml.contains("<ParameterKey>Stage</ParameterKey>"), "{xml}");
        assert!(xml.contains("<DefaultValue>dev</DefaultValue>"), "{xml}");
        assert!(xml.contains("<NoEcho>true</NoEcho>"), "{xml}");
        assert!(xml.contains("<member>CAPABILITY_IAM</member>"), "{xml}");
        assert!(xml.contains("<CapabilitiesReason>"), "{xml}");
        // ValidateTemplate does not carry types or constraints.
        assert!(!xml.contains("<ParameterType>"), "{xml}");
        assert!(!xml.contains("<ResourceTypes>"), "{xml}");

        let xml = get_template_summary_xml(&s);
        assert!(
            xml.contains("<ParameterType>String</ParameterType>"),
            "{xml}"
        );
        assert!(xml.contains("<member>AWS::IAM::Role</member>"), "{xml}");
        assert!(xml.contains("<AllowedValues>"), "{xml}");
        assert!(xml.contains("<member>prod</member>"), "{xml}");
        assert!(xml.contains("<Version>2010-09-09</Version>"), "{xml}");
    }

    #[test]
    fn xml_special_characters_are_escaped() {
        let s = summarize(
            "Parameters:\n  P:\n    Type: String\n    Description: \"a & b < c\"\nResources: {}\n",
        );
        let xml = get_template_summary_xml(&s);
        assert!(xml.contains("a &amp; b &lt; c"), "{xml}");
    }
}
