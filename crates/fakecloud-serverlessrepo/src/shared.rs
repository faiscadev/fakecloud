//! Primitives shared across the AWS Serverless Application Repository
//! (`serverlessrepo`) handlers: ARN / identifier synthesis, timestamps, the
//! back-referencing template URL, and SAM/CloudFormation template parsing.
//!
//! Kept in one place so the create / get paths cannot diverge on wire format,
//! and so the parameter-definition parser has a single implementation shared by
//! `CreateApplication`, `CreateApplicationVersion`, and `GetApplication`.

use serde_json::{json, Map, Value};

/// The application ARN,
/// `arn:aws:serverlessrepo:{region}:{account}:applications/{name}`. In SAR the
/// ARN *is* the `applicationId`.
pub fn application_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:serverlessrepo:{region}:{account}:applications/{name}")
}

/// A CloudFormation change-set id, `{stack}-{uuid}` shaped like the value
/// `CreateCloudFormationChangeSet` returns.
pub fn new_change_set_id() -> String {
    format!("arn:aws:cloudformation:changeSet/{}", uuid::Uuid::new_v4())
}

/// A CloudFormation stack id ARN of the form AWS mints for a SAR-launched
/// stack.
pub fn stack_id(region: &str, account: &str, stack_name: &str) -> String {
    format!(
        "arn:aws:cloudformation:{region}:{account}:stack/{stack_name}/{}",
        uuid::Uuid::new_v4()
    )
}

/// A CloudFormation template id (an opaque UUID).
pub fn new_template_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Current time as an ISO-8601 / RFC-3339 string. SAR timestamp members are
/// plain `__string`s carrying an ISO-8601 instant (e.g.
/// `2017-09-08T12:34:56.000Z`).
pub fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// An instant `hours` into the future as an ISO-8601 string, used for the
/// template expiration time.
pub fn iso_in_hours(hours: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::hours(hours))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// The URL a stored template is advertised under. Points back at this
/// fakecloud host (derived from the request `Host` header) so the value is a
/// well-formed, deterministic URL rather than a fabricated S3 presigned link.
///
/// Honest gap: fakecloud does not currently serve the raw template bytes at
/// this URL -- the SAR control plane stores the template body in memory but
/// there is no companion download route. The URL is structurally faithful.
pub fn template_url(host: &str, template_id: &str) -> String {
    let base = if host.is_empty() {
        "https://awsserverlessrepo-changesets.s3.amazonaws.com".to_string()
    } else if host.starts_with("http") {
        host.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", host.trim_end_matches('/'))
    };
    format!("{base}/serverlessrepo/templates/{template_id}")
}

/// Parse a CloudFormation / SAM template (JSON or YAML) into a `Value`.
/// Returns `None` when the body is empty or is not parseable as either format
/// (e.g. a synthetic placeholder string), so callers degrade to an empty
/// parameter set rather than erroring.
pub fn parse_template(body: &str) -> Option<Value> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        if v.is_object() {
            return Some(v);
        }
    }
    // YAML (the usual SAM authoring format). `serde_yaml` parses JSON too, but
    // we try JSON first to preserve exact numeric/precision semantics.
    if let Ok(v) = serde_yaml::from_str::<Value>(trimmed) {
        if v.is_object() {
            return Some(v);
        }
    }
    None
}

/// Build the `parameterDefinitions` list for a template's `Parameters` section.
/// Each entry carries the CloudFormation parameter's constraints plus the list
/// of logical resource ids that reference it (via `Ref` / `Fn::Sub`), which SAR
/// surfaces as `referencedByResources` (a required member of
/// `ParameterDefinition`).
pub fn parameter_definitions(template: &Value) -> Vec<Value> {
    let Some(params) = template.get("Parameters").and_then(Value::as_object) else {
        return Vec::new();
    };
    let resources = template.get("Resources").and_then(Value::as_object);
    let mut out = Vec::with_capacity(params.len());
    for (name, spec) in params {
        let mut def = Map::new();
        def.insert("name".into(), json!(name));
        // ReferencedByResources is a required member; always present (possibly
        // empty).
        def.insert(
            "referencedByResources".into(),
            json!(resources_referencing(resources, name)),
        );
        copy_str(&mut def, spec, "Type", "type");
        copy_str(&mut def, spec, "Default", "defaultValue");
        copy_str(&mut def, spec, "Description", "description");
        copy_str(&mut def, spec, "AllowedPattern", "allowedPattern");
        copy_str(
            &mut def,
            spec,
            "ConstraintDescription",
            "constraintDescription",
        );
        copy_int(&mut def, spec, "MaxLength", "maxLength");
        copy_int(&mut def, spec, "MinLength", "minLength");
        copy_int(&mut def, spec, "MaxValue", "maxValue");
        copy_int(&mut def, spec, "MinValue", "minValue");
        if let Some(b) = spec.get("NoEcho").and_then(Value::as_bool) {
            def.insert("noEcho".into(), json!(b));
        }
        if let Some(av) = spec.get("AllowedValues").and_then(Value::as_array) {
            let vals: Vec<Value> = av
                .iter()
                .filter_map(|v| v.as_str().map(|s| json!(s)))
                .collect();
            if !vals.is_empty() {
                def.insert("allowedValues".into(), json!(vals));
            }
        }
        out.push(Value::Object(def));
    }
    // Deterministic ordering for stable snapshots / round-trips.
    out.sort_by(|a, b| {
        a.get("name")
            .and_then(Value::as_str)
            .cmp(&b.get("name").and_then(Value::as_str))
    });
    out
}

/// Which logical resource ids reference `param` anywhere in their definition
/// (via `Ref`, `Fn::Sub`, or a bare `${param}` interpolation).
fn resources_referencing(resources: Option<&Map<String, Value>>, param: &str) -> Vec<String> {
    let Some(resources) = resources else {
        return Vec::new();
    };
    let mut ids: Vec<String> = resources
        .iter()
        .filter(|(_, body)| value_references_param(body, param))
        .map(|(id, _)| id.clone())
        .collect();
    ids.sort();
    ids
}

/// Recursively test whether a JSON subtree references the named parameter.
fn value_references_param(v: &Value, param: &str) -> bool {
    match v {
        Value::Object(map) => {
            if let Some(r) = map.get("Ref").and_then(Value::as_str) {
                if r == param {
                    return true;
                }
            }
            map.values().any(|c| value_references_param(c, param))
        }
        Value::Array(items) => items.iter().any(|c| value_references_param(c, param)),
        Value::String(s) => s.contains(&format!("${{{param}}}")),
        _ => false,
    }
}

/// The set of CloudFormation capabilities a template requires, derived from its
/// resource types. IAM resources require `CAPABILITY_IAM`
/// (`CAPABILITY_NAMED_IAM` when a resource sets an explicit name); a resource
/// policy requires `CAPABILITY_RESOURCE_POLICY`; nested applications /
/// transforms require `CAPABILITY_AUTO_EXPAND`.
pub fn required_capabilities(template: &Value) -> Vec<String> {
    let mut caps: Vec<String> = Vec::new();
    let resources = template.get("Resources").and_then(Value::as_object);
    let mut has_iam = false;
    let mut has_named_iam = false;
    let mut has_resource_policy = false;
    if let Some(resources) = resources {
        for body in resources.values() {
            let ty = body.get("Type").and_then(Value::as_str).unwrap_or("");
            if ty.contains("::IAM::") {
                has_iam = true;
                let named = body
                    .get("Properties")
                    .map(|p| {
                        p.get("RoleName").is_some()
                            || p.get("PolicyName").is_some()
                            || p.get("GroupName").is_some()
                            || p.get("UserName").is_some()
                            || p.get("ManagedPolicyName").is_some()
                    })
                    .unwrap_or(false);
                if named {
                    has_named_iam = true;
                }
            }
            // Serverless functions with inline policies expand into IAM roles.
            if ty == "AWS::Serverless::Function"
                && body
                    .get("Properties")
                    .and_then(|p| p.get("Policies"))
                    .is_some()
            {
                has_iam = true;
            }
            if ty.ends_with("Policy") && body.get("Properties").is_some() {
                // e.g. AWS::SQS::QueuePolicy, AWS::SNS::TopicPolicy.
                if ty.contains("::IAM::") {
                    // already handled
                } else {
                    has_resource_policy = true;
                }
            }
        }
    }
    // The SAM/serverless transform always auto-expands.
    let has_transform = template.get("Transform").is_some()
        || resources
            .map(|r| {
                r.values().any(|b| {
                    b.get("Type")
                        .and_then(Value::as_str)
                        .map(|t| t.starts_with("AWS::Serverless::"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
    if has_named_iam {
        caps.push("CAPABILITY_NAMED_IAM".to_string());
    } else if has_iam {
        caps.push("CAPABILITY_IAM".to_string());
    }
    if has_resource_policy {
        caps.push("CAPABILITY_RESOURCE_POLICY".to_string());
    }
    if has_transform {
        caps.push("CAPABILITY_AUTO_EXPAND".to_string());
    }
    caps
}

/// The nested-application dependencies a template declares via
/// `AWS::Serverless::Application` resources whose `Location` names an
/// application id + semantic version.
pub fn nested_application_dependencies(template: &Value) -> Vec<Value> {
    let Some(resources) = template.get("Resources").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for body in resources.values() {
        if body.get("Type").and_then(Value::as_str) != Some("AWS::Serverless::Application") {
            continue;
        }
        let Some(location) = body.get("Properties").and_then(|p| p.get("Location")) else {
            continue;
        };
        let app_id = location
            .get("ApplicationId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if app_id.is_empty() {
            continue;
        }
        let semver = location
            .get("SemanticVersion")
            .and_then(Value::as_str)
            .unwrap_or("1.0.0");
        out.push(json!({
            "applicationId": app_id,
            "semanticVersion": semver,
        }));
    }
    out
}

fn copy_str(def: &mut Map<String, Value>, spec: &Value, src: &str, dst: &str) {
    if let Some(v) = spec.get(src) {
        // CFN allows Default to be a number; SAR echoes defaultValue as a
        // string.
        let s = match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        };
        if let Some(s) = s {
            def.insert(dst.into(), json!(s));
        }
    }
}

fn copy_int(def: &mut Map<String, Value>, spec: &Value, src: &str, dst: &str) {
    if let Some(n) = spec.get(src).and_then(|v| match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }) {
        def.insert(dst.into(), json!(n));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAM: &str = r#"{
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Parameters": {
            "TableName": {
                "Type": "String",
                "Default": "items",
                "Description": "DynamoDB table",
                "AllowedValues": ["items", "things"],
                "MinLength": 1,
                "MaxLength": 64
            },
            "Unused": { "Type": "Number" }
        },
        "Resources": {
            "Fn": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "Policies": ["AmazonDynamoDBFullAccess"],
                    "Environment": { "Variables": { "TABLE": { "Ref": "TableName" } } }
                }
            }
        }
    }"#;

    #[test]
    fn parses_parameter_definitions() {
        let t = parse_template(SAM).unwrap();
        let defs = parameter_definitions(&t);
        assert_eq!(defs.len(), 2);
        let table = defs.iter().find(|d| d["name"] == "TableName").unwrap();
        assert_eq!(table["type"], "String");
        assert_eq!(table["defaultValue"], "items");
        assert_eq!(table["minLength"], 1);
        assert_eq!(table["allowedValues"], json!(["items", "things"]));
        assert_eq!(table["referencedByResources"], json!(["Fn"]));
        let unused = defs.iter().find(|d| d["name"] == "Unused").unwrap();
        assert_eq!(unused["referencedByResources"], json!([]));
    }

    #[test]
    fn derives_capabilities() {
        let t = parse_template(SAM).unwrap();
        let caps = required_capabilities(&t);
        assert!(caps.contains(&"CAPABILITY_IAM".to_string()));
        assert!(caps.contains(&"CAPABILITY_AUTO_EXPAND".to_string()));
    }

    #[test]
    fn non_template_body_is_none() {
        assert!(parse_template("not a template").is_none());
        assert!(parse_template("").is_none());
    }

    #[test]
    fn nested_deps_from_serverless_application() {
        let t = parse_template(
            r#"{"Resources":{"Nested":{"Type":"AWS::Serverless::Application",
               "Properties":{"Location":{"ApplicationId":"arn:aws:serverlessrepo:us-east-1:1:applications/x","SemanticVersion":"2.1.0"}}}}}"#,
        )
        .unwrap();
        let deps = nested_application_dependencies(&t);
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0]["semanticVersion"], "2.1.0");
    }
}
