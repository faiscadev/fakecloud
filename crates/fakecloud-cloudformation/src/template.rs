use serde_json::Value;
use std::collections::HashMap;

/// A parsed CloudFormation template.
#[derive(Debug, Clone)]
pub struct ParsedTemplate {
    pub description: Option<String>,
    pub resources: Vec<ResourceDefinition>,
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

/// Parse a CloudFormation template from a string (JSON or YAML).
pub fn parse_template(
    template_body: &str,
    parameters: &HashMap<String, String>,
) -> Result<ParsedTemplate, String> {
    parse_template_with_physical_ids(template_body, parameters, &HashMap::new())
}

/// Parse a CloudFormation template, resolving Refs using known physical resource IDs.
pub fn parse_template_with_physical_ids(
    template_body: &str,
    parameters: &HashMap<String, String>,
    resource_physical_ids: &HashMap<String, String>,
) -> Result<ParsedTemplate, String> {
    let value: Value = if template_body.trim_start().starts_with('{') {
        serde_json::from_str(template_body).map_err(|e| format!("Invalid JSON template: {e}"))?
    } else {
        serde_yaml::from_str(template_body).map_err(|e| format!("Invalid YAML template: {e}"))?
    };

    let description = value
        .get("Description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let resources_obj = value
        .get("Resources")
        .and_then(|v| v.as_object())
        .ok_or("Template must contain a Resources section")?;

    let mut resources = Vec::new();
    for (logical_id, resource) in resources_obj {
        let resource_type = resource
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or(format!("Resource {logical_id} must have a Type property"))?
            .to_string();

        let properties = resource
            .get("Properties")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        // Resolve Ref and parameter substitutions in properties
        let resolved = resolve_refs(
            &properties,
            parameters,
            resources_obj,
            resource_physical_ids,
        );

        resources.push(ResourceDefinition {
            logical_id: logical_id.clone(),
            resource_type,
            properties: resolved,
        });
    }

    Ok(ParsedTemplate {
        description,
        resources,
    })
}

/// Re-resolve a single resource definition's properties with updated physical IDs.
pub fn resolve_resource_properties(
    resource: &ResourceDefinition,
    template_body: &str,
    parameters: &HashMap<String, String>,
    resource_physical_ids: &HashMap<String, String>,
) -> Result<ResourceDefinition, String> {
    let value: Value = if template_body.trim_start().starts_with('{') {
        serde_json::from_str(template_body).map_err(|e| format!("Invalid JSON template: {e}"))?
    } else {
        serde_yaml::from_str(template_body).map_err(|e| format!("Invalid YAML template: {e}"))?
    };

    let resources_obj = value
        .get("Resources")
        .and_then(|v| v.as_object())
        .ok_or("Template must contain a Resources section")?;

    let raw_props = resources_obj
        .get(&resource.logical_id)
        .and_then(|r| r.get("Properties"))
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));

    let resolved = resolve_refs(&raw_props, parameters, resources_obj, resource_physical_ids);

    Ok(ResourceDefinition {
        logical_id: resource.logical_id.clone(),
        resource_type: resource.resource_type.clone(),
        properties: resolved,
    })
}

/// Resolve { "Ref": "param_name" } and { "Fn::GetAtt": [...] } in property values.
fn resolve_refs(
    value: &Value,
    parameters: &HashMap<String, String>,
    _resources: &serde_json::Map<String, Value>,
    resource_physical_ids: &HashMap<String, String>,
) -> Value {
    match value {
        Value::Object(map) => {
            if let Some(ref_val) = map.get("Ref") {
                if let Some(ref_name) = ref_val.as_str() {
                    // 1. Check explicit parameters first
                    if let Some(param_val) = parameters.get(ref_name) {
                        return Value::String(param_val.clone());
                    }
                    // 2. Check already-provisioned resource physical IDs
                    if let Some(physical_id) = resource_physical_ids.get(ref_name) {
                        return Value::String(physical_id.clone());
                    }
                    // 3. Allow pseudo-references to pass through as the ref name
                    if PSEUDO_REFS.contains(&ref_name) {
                        return Value::String(ref_name.to_string());
                    }
                    // 4. If it's a known logical resource in the template but not yet
                    //    provisioned, return the logical ID (will be resolved later
                    //    during incremental provisioning)
                    if _resources.contains_key(ref_name) {
                        return Value::String(ref_name.to_string());
                    }
                    // 5. Unknown ref — return as-is (could be a default parameter)
                    return Value::String(ref_name.to_string());
                }
            }
            if let Some(join_val) = map.get("Fn::Join") {
                if let Some(arr) = join_val.as_array() {
                    if arr.len() == 2 {
                        let delimiter = arr[0].as_str().unwrap_or("");
                        if let Some(parts) = arr[1].as_array() {
                            let resolved_parts: Vec<String> = parts
                                .iter()
                                .map(|p| {
                                    let resolved = resolve_refs(
                                        p,
                                        parameters,
                                        _resources,
                                        resource_physical_ids,
                                    );
                                    match resolved {
                                        Value::String(s) => s,
                                        other => other.to_string(),
                                    }
                                })
                                .collect();
                            return Value::String(resolved_parts.join(delimiter));
                        }
                    }
                }
            }
            if let Some(sub_val) = map.get("Fn::Sub") {
                if let Some(s) = sub_val.as_str() {
                    let mut result = s.to_string();
                    for (k, v) in parameters {
                        result = result.replace(&format!("${{{k}}}"), v);
                    }
                    // Also substitute resource physical IDs in Fn::Sub
                    for (k, v) in resource_physical_ids {
                        result = result.replace(&format!("${{{k}}}"), v);
                    }
                    return Value::String(result);
                }
            }
            // Recurse into object
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(
                    k.clone(),
                    resolve_refs(v, parameters, _resources, resource_physical_ids),
                );
            }
            Value::Object(new_map)
        }
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| resolve_refs(v, parameters, _resources, resource_physical_ids))
                .collect(),
        ),
        other => other.clone(),
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

        let parsed = parse_template(template, &HashMap::new()).unwrap();
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

        let parsed = parse_template(template, &HashMap::new()).unwrap();
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

        let mut params = HashMap::new();
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

        let mut physical_ids = HashMap::new();
        physical_ids.insert(
            "MyTopic".to_string(),
            "arn:aws:sns:us-east-1:123456789012:my-topic".to_string(),
        );

        let parsed =
            parse_template_with_physical_ids(template, &HashMap::new(), &physical_ids).unwrap();
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
        let parsed = parse_template(template, &HashMap::new()).unwrap();
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

        let parsed = parse_template(template, &HashMap::new()).unwrap();
        assert_eq!(
            parsed.resources[0].properties["QueueName"],
            Value::String("AWS::StackName".to_string())
        );
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

        let mut physical_ids = HashMap::new();
        physical_ids.insert(
            "MyTopic".to_string(),
            "arn:aws:sns:us-east-1:123456789012:my-topic".to_string(),
        );

        let parsed =
            parse_template_with_physical_ids(template, &HashMap::new(), &physical_ids).unwrap();
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
}
