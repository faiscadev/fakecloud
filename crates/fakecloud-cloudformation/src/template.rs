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

/// Parse a CloudFormation template from a string (JSON or YAML).
pub fn parse_template(
    template_body: &str,
    parameters: &HashMap<String, String>,
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
        let resolved = resolve_refs(&properties, parameters, resources_obj);

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

/// Resolve { "Ref": "param_name" } and { "Fn::GetAtt": [...] } in property values.
fn resolve_refs(
    value: &Value,
    parameters: &HashMap<String, String>,
    _resources: &serde_json::Map<String, Value>,
) -> Value {
    match value {
        Value::Object(map) => {
            if let Some(ref_val) = map.get("Ref") {
                if let Some(ref_name) = ref_val.as_str() {
                    if let Some(param_val) = parameters.get(ref_name) {
                        return Value::String(param_val.clone());
                    }
                    // If it references another resource, return logical ID as placeholder
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
                                    let resolved = resolve_refs(p, parameters, _resources);
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
                    return Value::String(result);
                }
            }
            // Recurse into object
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(k.clone(), resolve_refs(v, parameters, _resources));
            }
            Value::Object(new_map)
        }
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| resolve_refs(v, parameters, _resources))
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
}
