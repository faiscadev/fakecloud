//! SAM (`AWS::Serverless::Function`) `Events` + `Policies` expansion.
//!
//! `AWS::Serverless::Function` is sugar: its `Policies` become an implicit
//! execution role and its `Events` become the native trigger resources
//! (`Events::Rule`, `Lambda::EventSourceMapping`, `SNS::Subscription`, plus the
//! `Lambda::Permission` that lets the source invoke the function). The
//! transform previously dropped both — so a `sam deploy` produced a function
//! with no role (every IAM-enforced call denied) and no triggers (nothing ever
//! invoked it). This module synthesizes those native resources; the existing
//! provisioner arms then create them for real.
//!
//! API/HttpApi events are handled separately (implicit-API route synthesis).

use serde_json::{json, Map, Value};

/// One HTTP route a function exposes via an `Api`/`HttpApi` event, collected
/// during the function pass and turned into native API resources afterward by
/// [`synthesize_api_resources`].
pub(super) struct ApiRoute {
    pub function_id: String,
    /// Explicit `RestApiId`/`ApiId` (a `Ref`/string), or `None` for the
    /// implicit API SAM creates per protocol.
    pub explicit_api: Option<Value>,
    pub path: String,
    /// Upper-case HTTP method, or `ANY`.
    pub method: String,
    /// `true` = `HttpApi` (API Gateway v2), `false` = `Api` (REST v1).
    pub http_api: bool,
}

/// Expand a Serverless::Function's `Policies` and `Events`.
///
/// Mutates `lambda_props` in place (removes `Policies`/`Events`, sets `Role`
/// to the synthesized role when no explicit `Role` was given), returns the
/// extra native resources to add, and pushes any `Api`/`HttpApi` routes onto
/// `api_routes` for the post-pass implicit-API synthesis.
pub(super) fn expand_function_extras(
    function_id: &str,
    lambda_props: &mut Map<String, Value>,
    api_routes: &mut Vec<ApiRoute>,
) -> Vec<(String, Value)> {
    let mut extras: Vec<(String, Value)> = Vec::new();

    // --- Policies -> implicit execution role ---
    let policies = lambda_props.remove("Policies");
    let has_explicit_role = lambda_props
        .get("Role")
        .map(|r| !r.is_null())
        .unwrap_or(false);
    if !has_explicit_role {
        let role_id = format!("{function_id}Role");
        let role = build_execution_role(policies.as_ref());
        extras.push((role_id.clone(), role));
        lambda_props.insert(
            "Role".to_string(),
            json!({ "Fn::GetAtt": [role_id, "Arn"] }),
        );
    }

    // --- Events -> native trigger resources ---
    let Some(events) = lambda_props.remove("Events") else {
        return extras;
    };
    let Some(events) = events.as_object().cloned() else {
        return extras;
    };
    for (event_name, event) in &events {
        let Some(event_obj) = event.as_object() else {
            continue;
        };
        let event_type = event_obj.get("Type").and_then(|v| v.as_str()).unwrap_or("");
        let props = event_obj
            .get("Properties")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let id_base = format!("{function_id}{event_name}");
        match event_type {
            "Schedule" | "ScheduleV2" => {
                extras.extend(schedule_event(function_id, &id_base, &props));
            }
            "SQS" | "DynamoDB" | "Kinesis" | "MSK" | "MQ" => {
                extras.push(event_source_mapping(
                    function_id,
                    &id_base,
                    event_type,
                    &props,
                ));
            }
            "SNS" => {
                extras.extend(sns_event(function_id, &id_base, &props));
            }
            "EventBridgeRule" | "CloudWatchEvent" => {
                extras.extend(eventbridge_event(function_id, &id_base, &props));
            }
            "Api" | "HttpApi" => {
                // Collect the route; the API resources are synthesized in a
                // post-pass so all functions sharing the implicit API land in
                // one RestApi/HttpApi.
                let http_api = event_type == "HttpApi";
                let path = props
                    .get("Path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("/")
                    .to_string();
                let method = props
                    .get("Method")
                    .and_then(|v| v.as_str())
                    .unwrap_or("ANY")
                    .to_uppercase();
                let explicit_api = props
                    .get(if http_api { "ApiId" } else { "RestApiId" })
                    .cloned();
                api_routes.push(ApiRoute {
                    function_id: function_id.to_string(),
                    explicit_api,
                    path,
                    method,
                    http_api,
                });
            }
            // S3 / Cognito / etc.: left for a dedicated pass.
            _ => {}
        }
    }

    extras
}

/// Build an `AWS::IAM::Role` from a function's `Policies`. The trust policy
/// always allows `lambda.amazonaws.com` to assume it. `Policies` entries that
/// are managed-policy names/ARNs become `ManagedPolicyArns`; inline statement
/// documents become inline `Policies`. SAM policy *templates* (object with a
/// single template key like `DynamoDBCrudPolicy`) are not expanded here — the
/// role is still created (assume-role works) so the function is no longer
/// role-less.
fn build_execution_role(policies: Option<&Value>) -> Value {
    let mut managed_arns: Vec<Value> = vec![json!(
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    )];
    let mut inline: Vec<Value> = Vec::new();

    let mut handle = |p: &Value| {
        if let Some(name) = p.as_str() {
            managed_arns.push(json!(managed_policy_arn(name)));
        } else if let Some(obj) = p.as_object() {
            // An inline statement document: {Statement: [...]} or a full policy.
            if obj.contains_key("Statement") {
                inline.push(json!({
                    "PolicyName": "InlinePolicy",
                    "PolicyDocument": p.clone(),
                }));
            }
            // Otherwise a SAM policy template — not expanded; role still created.
        }
    };

    match policies {
        Some(Value::Array(arr)) => arr.iter().for_each(&mut handle),
        Some(p) => handle(p),
        None => {}
    }

    let mut props = json!({
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "lambda.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        },
        "ManagedPolicyArns": managed_arns,
    });
    if !inline.is_empty() {
        props["Policies"] = json!(inline);
    }
    json!({ "Type": "AWS::IAM::Role", "Properties": props })
}

/// Resolve a managed-policy reference to a full ARN. A bare name (e.g.
/// `AmazonS3ReadOnlyAccess`) becomes the AWS-managed ARN; anything already in
/// `arn:` form passes through.
fn managed_policy_arn(name: &str) -> String {
    if name.starts_with("arn:") {
        name.to_string()
    } else {
        format!("arn:aws:iam::aws:policy/{name}")
    }
}

/// `Schedule` event -> `Events::Rule` (ScheduleExpression) targeting the
/// function + a `Lambda::Permission` allowing EventBridge to invoke it.
fn schedule_event(
    function_id: &str,
    id_base: &str,
    props: &Map<String, Value>,
) -> Vec<(String, Value)> {
    let schedule = props
        .get("Schedule")
        .or_else(|| props.get("ScheduleExpression"))
        .cloned()
        .unwrap_or(json!("rate(1 day)"));
    let rule_id = format!("{id_base}Rule");
    let mut rule_props = json!({
        "ScheduleExpression": schedule,
        "State": props.get("Enabled").map(|e| if e.as_bool() == Some(false) { json!("DISABLED") } else { json!("ENABLED") }).unwrap_or(json!("ENABLED")),
        "Targets": [{
            "Id": format!("{function_id}Target"),
            "Arn": { "Fn::GetAtt": [function_id, "Arn"] }
        }]
    });
    if let Some(name) = props.get("Name") {
        rule_props["Name"] = name.clone();
    }
    if let Some(input) = props.get("Input") {
        rule_props["Targets"][0]["Input"] = input.clone();
    }
    vec![
        (
            rule_id.clone(),
            json!({ "Type": "AWS::Events::Rule", "Properties": rule_props }),
        ),
        (
            format!("{id_base}Permission"),
            lambda_permission(
                function_id,
                "events.amazonaws.com",
                json!({ "Fn::GetAtt": [rule_id, "Arn"] }),
            ),
        ),
    ]
}

/// SQS / DynamoDB / Kinesis / MSK / MQ event -> `Lambda::EventSourceMapping`.
fn event_source_mapping(
    function_id: &str,
    id_base: &str,
    event_type: &str,
    props: &Map<String, Value>,
) -> (String, Value) {
    // The source ARN property name varies by event type.
    let source = props
        .get("Queue")
        .or_else(|| props.get("Stream"))
        .or_else(|| props.get("EventSourceArn"))
        .or_else(|| props.get("Topics"))
        .cloned()
        .unwrap_or(Value::Null);
    let mut esm = json!({
        "FunctionName": { "Ref": function_id },
        "EventSourceArn": source,
    });
    if let Some(bs) = props.get("BatchSize") {
        esm["BatchSize"] = bs.clone();
    }
    if let Some(en) = props.get("Enabled") {
        esm["Enabled"] = en.clone();
    }
    // Stream sources need a StartingPosition; SAM defaults to TRIM_HORIZON.
    if matches!(event_type, "DynamoDB" | "Kinesis" | "MSK") {
        esm["StartingPosition"] = props
            .get("StartingPosition")
            .cloned()
            .unwrap_or(json!("TRIM_HORIZON"));
    }
    (
        format!("{id_base}EventSourceMapping"),
        json!({ "Type": "AWS::Lambda::EventSourceMapping", "Properties": esm }),
    )
}

/// SNS event -> `SNS::Subscription` (protocol lambda) + invoke permission.
fn sns_event(function_id: &str, id_base: &str, props: &Map<String, Value>) -> Vec<(String, Value)> {
    let topic = props.get("Topic").cloned().unwrap_or(Value::Null);
    let sub = json!({
        "Type": "AWS::SNS::Subscription",
        "Properties": {
            "TopicArn": topic.clone(),
            "Protocol": "lambda",
            "Endpoint": { "Fn::GetAtt": [function_id, "Arn"] }
        }
    });
    vec![
        (format!("{id_base}Subscription"), sub),
        (
            format!("{id_base}Permission"),
            lambda_permission(function_id, "sns.amazonaws.com", topic),
        ),
    ]
}

/// EventBridgeRule / CloudWatchEvent event -> `Events::Rule` (EventPattern)
/// targeting the function + invoke permission.
fn eventbridge_event(
    function_id: &str,
    id_base: &str,
    props: &Map<String, Value>,
) -> Vec<(String, Value)> {
    let rule_id = format!("{id_base}Rule");
    let mut rule_props = json!({
        "Targets": [{
            "Id": format!("{function_id}Target"),
            "Arn": { "Fn::GetAtt": [function_id, "Arn"] }
        }]
    });
    if let Some(pattern) = props.get("Pattern").or_else(|| props.get("EventPattern")) {
        rule_props["EventPattern"] = pattern.clone();
    }
    if let Some(bus) = props.get("EventBusName") {
        rule_props["EventBusName"] = bus.clone();
    }
    vec![
        (
            rule_id.clone(),
            json!({ "Type": "AWS::Events::Rule", "Properties": rule_props }),
        ),
        (
            format!("{id_base}Permission"),
            lambda_permission(
                function_id,
                "events.amazonaws.com",
                json!({ "Fn::GetAtt": [rule_id, "Arn"] }),
            ),
        ),
    ]
}

/// Build an `AWS::Lambda::Permission` granting `principal` invoke on the
/// function, scoped to `source_arn`.
fn lambda_permission(function_id: &str, principal: &str, source_arn: Value) -> Value {
    json!({
        "Type": "AWS::Lambda::Permission",
        "Properties": {
            "FunctionName": { "Ref": function_id },
            "Action": "lambda:InvokeFunction",
            "Principal": principal,
            "SourceArn": source_arn
        }
    })
}

/// Strip a path/method into a logical-id-safe token (alphanumeric only).
fn sanitize(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

/// The AWS_PROXY integration URI for a Lambda, resolved at provision time via
/// `Fn::Sub` over the function's ARN.
fn lambda_integration_uri(function_id: &str) -> Value {
    json!({
        "Fn::Sub": format!(
            "arn:aws:apigateway:${{AWS::Region}}:lambda:path/2015-03-31/functions/${{{function_id}.Arn}}/invocations"
        )
    })
}

/// Turn the collected `Api`/`HttpApi` routes into native API resources. All
/// routes without an explicit API id share one implicit API per protocol
/// (`ServerlessRestApi` / `ServerlessHttpApi`), matching SAM. Returns the extra
/// resources keyed by logical id. Without this a SAM function with an Api event
/// deployed with no routes — every call 404'd (bug-hunt 2026-06-24, 1.1).
pub(super) fn synthesize_api_resources(routes: &[ApiRoute]) -> Vec<(String, Value)> {
    let mut out: Vec<(String, Value)> = Vec::new();
    if routes.is_empty() {
        return out;
    }

    // --- REST (v1) routes sharing the implicit ServerlessRestApi ---
    let rest: Vec<&ApiRoute> = routes
        .iter()
        .filter(|r| !r.http_api && r.explicit_api.is_none())
        .collect();
    if !rest.is_empty() {
        let api_id = "ServerlessRestApi";
        out.push((
            api_id.to_string(),
            json!({
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": { "Name": "ServerlessRestApi", "EndpointConfiguration": { "Types": ["REGIONAL"] } }
            }),
        ));
        // Build the resource tree, deduping by full path prefix.
        let mut resource_for_path: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        let mut method_ids: Vec<String> = Vec::new();
        for route in &rest {
            // Resolve (creating as needed) the Resource for this path, then a
            // Method + Permission targeting the function.
            let mut parent_ref = json!({ "Fn::GetAtt": [api_id, "RootResourceId"] });
            let mut prefix = String::new();
            for segment in route.path.split('/').filter(|s| !s.is_empty()) {
                prefix.push('/');
                prefix.push_str(segment);
                let res_id = format!("{api_id}Resource{}", sanitize(&prefix));
                if !resource_for_path.contains_key(&prefix) {
                    out.push((
                        res_id.clone(),
                        json!({
                            "Type": "AWS::ApiGateway::Resource",
                            "Properties": {
                                "RestApiId": { "Ref": api_id },
                                "ParentId": parent_ref,
                                "PathPart": segment,
                            }
                        }),
                    ));
                    resource_for_path.insert(prefix.clone(), res_id.clone());
                }
                let id = resource_for_path.get(&prefix).cloned().unwrap();
                parent_ref = json!({ "Fn::GetAtt": [id, "ResourceId"] });
            }
            // Root path "/" maps to the RestApi's root resource id directly.
            let resource_ref = if route.path.trim_matches('/').is_empty() {
                json!({ "Fn::GetAtt": [api_id, "RootResourceId"] })
            } else {
                json!({ "Ref": resource_for_path.get(&prefix).cloned().unwrap() })
            };
            let http_method = if route.method == "ANY" {
                "ANY".to_string()
            } else {
                route.method.clone()
            };
            let method_id = format!(
                "{api_id}Method{}{}{}",
                sanitize(&route.function_id),
                sanitize(&route.path),
                sanitize(&http_method)
            );
            out.push((
                method_id.clone(),
                json!({
                    "Type": "AWS::ApiGateway::Method",
                    "Properties": {
                        "RestApiId": { "Ref": api_id },
                        "ResourceId": resource_ref,
                        "HttpMethod": http_method,
                        "AuthorizationType": "NONE",
                        "Integration": {
                            "Type": "AWS_PROXY",
                            "IntegrationHttpMethod": "POST",
                            "Uri": lambda_integration_uri(&route.function_id),
                        }
                    }
                }),
            ));
            method_ids.push(method_id);
            out.push((
                format!("{api_id}Perm{}", sanitize(&route.function_id)),
                lambda_permission(
                    &route.function_id,
                    "apigateway.amazonaws.com",
                    json!({ "Fn::Sub": format!("arn:aws:execute-api:${{AWS::Region}}:${{AWS::AccountId}}:${{{api_id}}}/*") }),
                ),
            ));
        }
        // One Deployment (after all methods) + Stage.
        out.push((
            format!("{api_id}Deployment"),
            json!({
                "Type": "AWS::ApiGateway::Deployment",
                "DependsOn": method_ids,
                "Properties": { "RestApiId": { "Ref": api_id } }
            }),
        ));
        out.push((
            format!("{api_id}ProdStage"),
            json!({
                "Type": "AWS::ApiGateway::Stage",
                "Properties": {
                    "RestApiId": { "Ref": api_id },
                    "DeploymentId": { "Ref": format!("{api_id}Deployment") },
                    "StageName": "Prod",
                }
            }),
        ));
    }

    // --- HTTP (v2) routes sharing the implicit ServerlessHttpApi ---
    let http: Vec<&ApiRoute> = routes
        .iter()
        .filter(|r| r.http_api && r.explicit_api.is_none())
        .collect();
    if !http.is_empty() {
        let api_id = "ServerlessHttpApi";
        out.push((
            api_id.to_string(),
            json!({
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": { "Name": "ServerlessHttpApi", "ProtocolType": "HTTP" }
            }),
        ));
        for route in &http {
            let integ_id = format!("{api_id}Integ{}", sanitize(&route.function_id));
            out.push((
                integ_id.clone(),
                json!({
                    "Type": "AWS::ApiGatewayV2::Integration",
                    "Properties": {
                        "ApiId": { "Ref": api_id },
                        "IntegrationType": "AWS_PROXY",
                        "IntegrationUri": { "Fn::GetAtt": [route.function_id.clone(), "Arn"] },
                        "PayloadFormatVersion": "2.0",
                    }
                }),
            ));
            let route_key = if route.method == "ANY" {
                format!("ANY {}", route.path)
            } else {
                format!("{} {}", route.method, route.path)
            };
            out.push((
                format!(
                    "{api_id}Route{}{}",
                    sanitize(&route.function_id),
                    sanitize(&route.path)
                ),
                json!({
                    "Type": "AWS::ApiGatewayV2::Route",
                    "Properties": {
                        "ApiId": { "Ref": api_id },
                        "RouteKey": route_key,
                        "Target": { "Fn::Sub": format!("integrations/${{{integ_id}}}") },
                    }
                }),
            ));
            out.push((
                format!("{api_id}Perm{}", sanitize(&route.function_id)),
                lambda_permission(
                    &route.function_id,
                    "apigateway.amazonaws.com",
                    json!({ "Fn::Sub": format!("arn:aws:execute-api:${{AWS::Region}}:${{AWS::AccountId}}:${{{api_id}}}/*") }),
                ),
            ));
        }
        out.push((
            format!("{api_id}DefaultStage"),
            json!({
                "Type": "AWS::ApiGatewayV2::Stage",
                "Properties": { "ApiId": { "Ref": api_id }, "StageName": "$default", "AutoDeploy": true }
            }),
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_become_execution_role() {
        let mut props = serde_json::from_value::<Map<String, Value>>(json!({
            "Handler": "index.handler",
            "Policies": ["AmazonS3ReadOnlyAccess", {"Statement": [{"Effect":"Allow","Action":"logs:PutLogEvents","Resource":"*"}]}]
        }))
        .unwrap();
        let extras = expand_function_extras("MyFn", &mut props, &mut Vec::new());
        // Role injected as GetAtt.
        assert_eq!(props["Role"], json!({"Fn::GetAtt": ["MyFnRole", "Arn"]}));
        assert!(props.get("Policies").is_none());
        let (rid, role) = extras.iter().find(|(id, _)| id == "MyFnRole").unwrap();
        assert_eq!(rid, "MyFnRole");
        let arns = role["Properties"]["ManagedPolicyArns"].as_array().unwrap();
        assert!(arns
            .iter()
            .any(|a| a == "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"));
        assert!(role["Properties"]["Policies"].as_array().unwrap().len() == 1);
    }

    #[test]
    fn explicit_role_is_kept() {
        let mut props = serde_json::from_value::<Map<String, Value>>(json!({
            "Role": "arn:aws:iam::123456789012:role/explicit",
            "Policies": ["AmazonS3ReadOnlyAccess"]
        }))
        .unwrap();
        let extras = expand_function_extras("MyFn", &mut props, &mut Vec::new());
        assert_eq!(
            props["Role"],
            json!("arn:aws:iam::123456789012:role/explicit")
        );
        assert!(!extras.iter().any(|(id, _)| id == "MyFnRole"));
    }

    #[test]
    fn schedule_event_makes_rule_and_permission() {
        let mut props = serde_json::from_value::<Map<String, Value>>(json!({
            "Events": { "Cron": { "Type": "Schedule", "Properties": { "Schedule": "rate(5 minutes)" } } }
        }))
        .unwrap();
        let extras = expand_function_extras("MyFn", &mut props, &mut Vec::new());
        let (_, rule) = extras.iter().find(|(id, _)| id == "MyFnCronRule").unwrap();
        assert_eq!(rule["Type"], "AWS::Events::Rule");
        assert_eq!(rule["Properties"]["ScheduleExpression"], "rate(5 minutes)");
        assert_eq!(
            rule["Properties"]["Targets"][0]["Arn"],
            json!({"Fn::GetAtt": ["MyFn","Arn"]})
        );
        let (_, perm) = extras
            .iter()
            .find(|(id, _)| id == "MyFnCronPermission")
            .unwrap();
        assert_eq!(perm["Properties"]["Principal"], "events.amazonaws.com");
    }

    #[test]
    fn sqs_event_makes_event_source_mapping() {
        let mut props = serde_json::from_value::<Map<String, Value>>(json!({
            "Events": { "Q": { "Type": "SQS", "Properties": { "Queue": "arn:aws:sqs:us-east-1:000000000000:q", "BatchSize": 10 } } }
        }))
        .unwrap();
        let extras = expand_function_extras("MyFn", &mut props, &mut Vec::new());
        let (_, esm) = extras
            .iter()
            .find(|(id, _)| id == "MyFnQEventSourceMapping")
            .unwrap();
        assert_eq!(esm["Type"], "AWS::Lambda::EventSourceMapping");
        assert_eq!(
            esm["Properties"]["EventSourceArn"],
            "arn:aws:sqs:us-east-1:000000000000:q"
        );
        assert_eq!(esm["Properties"]["BatchSize"], 10);
        assert_eq!(esm["Properties"]["FunctionName"], json!({"Ref": "MyFn"}));
    }
}
