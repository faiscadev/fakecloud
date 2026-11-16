//! SAM (`AWS::Serverless::Function`) `Events` + `Policies` expansion.
//!
//! `AWS::Serverless::Function` is sugar: its `Policies` become an implicit
//! execution role, `AutoPublishAlias` a published version + alias,
//! `DeploymentPreference` a CodeDeploy application + deployment group,
//! `FunctionUrlConfig` a function URL, and its `Events` the native trigger
//! resources (`Events::Rule`, `Lambda::EventSourceMapping`,
//! `SNS::Subscription`, `Logs::SubscriptionFilter`, `IoT::TopicRule`, bucket
//! and user-pool trigger configuration, plus the `Lambda::Permission` that
//! lets each source invoke the function). This module synthesizes those
//! native resources; the existing provisioner arms then create them for real.
//!
//! API/HttpApi events are collected as [`ApiRoute`]s and built onto their API
//! by [`super::sam_api`].

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

/// How the resources an event synthesizes address the function: the function
/// itself, or -- under `AutoPublishAlias` -- its alias, as SAM points every
/// event source at the alias.
#[derive(Debug, Clone)]
pub(super) struct FnTarget {
    pub function_id: String,
    /// Value for a `FunctionName` property (`Ref` to the function or alias).
    pub name_ref: Value,
    /// Value for an ARN property (`GetAtt Fn.Arn`, or `Ref` to the alias,
    /// which resolves to the alias ARN).
    pub arn: Value,
    /// `Fn::Sub` token resolving to the same ARN.
    pub sub_token: String,
}

impl FnTarget {
    fn function(function_id: &str) -> Self {
        Self {
            function_id: function_id.to_string(),
            name_ref: json!({ "Ref": function_id }),
            arn: json!({ "Fn::GetAtt": [function_id, "Arn"] }),
            sub_token: format!("${{{function_id}.Arn}}"),
        }
    }

    fn alias(function_id: &str, alias_id: &str) -> Self {
        Self {
            function_id: function_id.to_string(),
            name_ref: json!({ "Ref": alias_id }),
            arn: json!({ "Ref": alias_id }),
            sub_token: format!("${{{alias_id}}}"),
        }
    }
}

/// One HTTP route a function exposes via an `Api`/`HttpApi` event, collected
/// during the function pass and built onto its API afterward by
/// [`super::sam_api`].
#[derive(Debug, Clone)]
pub(super) struct ApiRoute {
    pub target: FnTarget,
    /// `<Function><EventName>`, the base of the logical ids the route's
    /// resources get.
    pub id_base: String,
    pub event_name: String,
    /// Logical id of the API: an `AWS::Serverless::Api`/`HttpApi` named by
    /// `RestApiId`/`ApiId`, or the implicit `ServerlessRestApi` /
    /// `ServerlessHttpApi`.
    pub api_id: String,
    /// The route's path, or `None` for an HttpApi event without one (the
    /// `$default` route).
    pub path: Option<String>,
    /// Upper-case HTTP method, or `ANY`.
    pub method: String,
    /// `true` = `HttpApi` (API Gateway v2), `false` = `Api` (REST v1).
    pub http_api: bool,
    /// The event's `Auth` block.
    pub auth: Option<Map<String, Value>>,
    /// HttpApi `PayloadFormatVersion` / `TimeoutInMillis`.
    pub payload_format_version: Option<Value>,
    pub timeout_in_millis: Option<Value>,
    /// The route's `x-amazon-apigateway-integration` when it is not a Lambda
    /// proxy integration (a state machine `Api` event's StartExecution
    /// service integration); such a route needs no invoke permission.
    pub integration: Option<Value>,
}

/// Template-wide context the function pass reads and records into: which
/// explicit APIs exist, the routes bound for them, and the edits events make
/// to other resources (bucket notifications, user-pool triggers).
#[derive(Default)]
pub(super) struct SamContext {
    /// The original template's resources, to validate event references.
    pub resources: Map<String, Value>,
    pub api_routes: Vec<ApiRoute>,
    /// `bucket logical id -> (LambdaConfiguration, permission id)`.
    pub bucket_notifications: Vec<(String, Value, String)>,
    /// `(user pool logical id, trigger, function ARN, event id)`.
    pub user_pool_triggers: Vec<(String, String, Value, String)>,
    /// `Function.Alias` / `Function.Version` -> the logical id SAM's
    /// `Ref MyFunction.Alias` syntax resolves to.
    pub ref_aliases: Vec<(String, String)>,
}

impl SamContext {
    fn resource_type(&self, id: &str) -> Option<&str> {
        self.resources
            .get(id)
            .and_then(|r| r.get("Type"))
            .and_then(Value::as_str)
    }
}

/// The logical id a `{ "Ref": "Id" }` value names.
fn ref_id(v: Option<&Value>) -> Option<&str> {
    v.and_then(|v| v.get("Ref")).and_then(Value::as_str)
}

fn invalid_event(function_id: &str, event_name: &str, msg: &str) -> String {
    format!(
        "Transform AWS::Serverless-2016-10-31 failed: Resource with id [{function_id}] is invalid. \
         Event with id [{event_name}] is invalid. {msg}"
    )
}

fn invalid_resource(function_id: &str, msg: &str) -> String {
    format!(
        "Transform AWS::Serverless-2016-10-31 failed: Resource with id [{function_id}] is invalid. {msg}"
    )
}

/// Function properties SAM consumes and native `AWS::Lambda::Function` does
/// not know.
const SAM_ONLY_FUNCTION_PROPS: &[&str] = &[
    "AutoPublishAlias",
    "AutoPublishCodeSha256",
    "AutoPublishAliasAllProperties",
    "VersionDescription",
    "DeploymentPreference",
    "ProvisionedConcurrencyConfig",
    "FunctionUrlConfig",
];

/// The 10-hex-digit content hash SAM suffixes a published version's logical
/// id with, so changed code publishes a new version.
fn version_hash(lambda_props: &Map<String, Value>, sha: Option<&Value>, all: bool) -> String {
    let mut hasher = Sha256::new();
    if all {
        hasher.update(Value::Object(lambda_props.clone()).to_string());
    } else {
        hasher.update(
            lambda_props
                .get("Code")
                .cloned()
                .unwrap_or(Value::Null)
                .to_string(),
        );
    }
    if let Some(sha) = sha {
        hasher.update(sha.to_string());
    }
    let digest = hasher.finalize();
    digest.iter().take(5).map(|b| format!("{b:02x}")).collect()
}

/// Expand a Serverless::Function's `Policies`, `AutoPublishAlias` (+
/// `ProvisionedConcurrencyConfig`, `DeploymentPreference`),
/// `FunctionUrlConfig` and `Events`.
///
/// Mutates `lambda_props` in place (removes the SAM-only properties, sets
/// `Role` to the synthesized role when no explicit `Role` was given), returns
/// the extra native resources to add, and records API routes and
/// cross-resource edits in `ctx`. An event type SAM does not define, or an
/// event that references something it cannot, fails the transform.
pub(super) fn expand_function_extras(
    function_id: &str,
    lambda_props: &mut Map<String, Value>,
    ctx: &mut SamContext,
) -> Result<Vec<(String, Value)>, String> {
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

    let events = lambda_props.remove("Events");
    let sam: Map<String, Value> = SAM_ONLY_FUNCTION_PROPS
        .iter()
        .filter_map(|k| lambda_props.remove(*k).map(|v| (k.to_string(), v)))
        .collect();

    // --- AutoPublishAlias -> Version + Alias ---
    let alias_name = match sam.get("AutoPublishAlias") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::String(_)) | None => None,
        Some(_) => {
            return Err(invalid_resource(
                function_id,
                "'AutoPublishAlias' must be a string.",
            ))
        }
    };
    let target = match &alias_name {
        Some(alias) => {
            let all = sam
                .get("AutoPublishAliasAllProperties")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let version_id = format!(
                "{function_id}Version{}",
                version_hash(lambda_props, sam.get("AutoPublishCodeSha256"), all)
            );
            let mut version_props = json!({ "FunctionName": { "Ref": function_id } });
            if let Some(desc) = sam.get("VersionDescription") {
                version_props["Description"] = desc.clone();
            }
            extras.push((
                version_id.clone(),
                json!({
                    "Type": "AWS::Lambda::Version",
                    "DeletionPolicy": "Retain",
                    "Properties": version_props,
                }),
            ));
            let alias_id = format!(
                "{function_id}Alias{}",
                alias
                    .chars()
                    .filter(char::is_ascii_alphanumeric)
                    .collect::<String>()
            );
            let mut alias_props = json!({
                "Name": alias,
                "FunctionName": { "Ref": function_id },
                "FunctionVersion": { "Fn::GetAtt": [version_id, "Version"] },
            });
            if let Some(pc) = sam.get("ProvisionedConcurrencyConfig") {
                alias_props["ProvisionedConcurrencyConfig"] = pc.clone();
            }
            let mut alias_resource =
                json!({ "Type": "AWS::Lambda::Alias", "Properties": alias_props });
            if let Some(pref) = sam.get("DeploymentPreference") {
                let (deployment_extras, update_policy) = deployment_preference(function_id, pref)?;
                extras.extend(deployment_extras);
                if let Some(policy) = update_policy {
                    alias_resource["UpdatePolicy"] = policy;
                }
            }
            extras.push((alias_id.clone(), alias_resource));
            ctx.ref_aliases
                .push((format!("{function_id}.Alias"), alias_id.clone()));
            ctx.ref_aliases
                .push((format!("{function_id}.Version"), version_id));
            FnTarget::alias(function_id, &alias_id)
        }
        None => {
            if sam.contains_key("ProvisionedConcurrencyConfig") {
                return Err(invalid_resource(
                    function_id,
                    "To set ProvisionedConcurrencyConfig AutoPublishALias must be defined on the function",
                ));
            }
            if sam.contains_key("DeploymentPreference") {
                return Err(invalid_resource(
                    function_id,
                    "'DeploymentPreference' requires AutoPublishAlias property to be specified.",
                ));
            }
            FnTarget::function(function_id)
        }
    };

    // --- FunctionUrlConfig -> Lambda::Url (+ public permission) ---
    if let Some(url_cfg) = sam.get("FunctionUrlConfig") {
        let auth_type = url_cfg
            .get("AuthType")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                invalid_resource(function_id, "AuthType is required to configure function property `FunctionUrlConfig`. Please provide either AWS_IAM or NONE.")
            })?;
        let mut url_props = json!({
            "TargetFunctionArn": { "Ref": function_id },
            "AuthType": auth_type,
        });
        if let Some(alias) = &alias_name {
            url_props["Qualifier"] = json!(alias);
        }
        for key in ["Cors", "InvokeMode"] {
            if let Some(v) = url_cfg.get(key) {
                url_props[key] = v.clone();
            }
        }
        extras.push((
            format!("{function_id}Url"),
            json!({ "Type": "AWS::Lambda::Url", "Properties": url_props }),
        ));
        if auth_type == "NONE" {
            extras.push((
                format!("{function_id}UrlPublicPermissions"),
                json!({
                    "Type": "AWS::Lambda::Permission",
                    "Properties": {
                        "Action": "lambda:InvokeFunctionUrl",
                        "FunctionName": target.name_ref.clone(),
                        "Principal": "*",
                        "FunctionUrlAuthType": "NONE",
                    }
                }),
            ));
        }
    }

    // --- Events -> native trigger resources ---
    let Some(events) = events else {
        return Ok(extras);
    };
    let Some(events) = events.as_object().cloned() else {
        return Err(invalid_resource(function_id, "'Events' must be a map."));
    };
    for (event_name, event) in &events {
        let Some(event_obj) = event.as_object() else {
            return Err(invalid_event(
                function_id,
                event_name,
                "Event must be an object.",
            ));
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
                extras.extend(schedule_event(&target, &id_base, &props));
            }
            "SQS" | "DynamoDB" | "Kinesis" | "MSK" | "MQ" | "SelfManagedKafka" | "DocumentDB" => {
                extras.push(event_source_mapping(&target, &id_base, event_type, &props));
            }
            "SNS" => {
                extras.extend(sns_event(&target, &id_base, &props));
            }
            "EventBridgeRule" | "CloudWatchEvent" => {
                extras.extend(eventbridge_event(&target, &id_base, &props));
            }
            "S3" => {
                extras.extend(s3_event(&target, event_name, &id_base, &props, ctx)?);
            }
            "CloudWatchLogs" => {
                extras.extend(cloudwatch_logs_event(
                    &target, event_name, &id_base, &props,
                )?);
            }
            "Cognito" => {
                extras.extend(cognito_event(&target, event_name, &id_base, &props, ctx)?);
            }
            "IoTRule" => {
                extras.extend(iot_rule_event(&target, event_name, &id_base, &props)?);
            }
            "AlexaSkill" => {
                let mut perm = lambda_permission(&target, "alexa-appkit.amazon.com", None);
                if let Some(skill) = props.get("SkillId") {
                    perm["Properties"]["EventSourceToken"] = skill.clone();
                }
                extras.push((format!("{id_base}Permission"), perm));
            }
            "Api" | "HttpApi" => {
                let http_api = event_type == "HttpApi";
                let (api_key, implicit, sam_type) = if http_api {
                    ("ApiId", "ServerlessHttpApi", "AWS::Serverless::HttpApi")
                } else {
                    ("RestApiId", "ServerlessRestApi", "AWS::Serverless::Api")
                };
                let api_id = match props.get(api_key) {
                    None => implicit.to_string(),
                    Some(v) => {
                        let id = ref_id(Some(v))
                            .filter(|id| ctx.resource_type(id) == Some(sam_type))
                            .ok_or_else(|| {
                                invalid_event(
                                    function_id,
                                    event_name,
                                    &format!("{api_key} must be a valid reference to an '{sam_type}' resource in same template."),
                                )
                            })?;
                        id.to_string()
                    }
                };
                let path = props
                    .get("Path")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let method = props
                    .get("Method")
                    .and_then(|v| v.as_str())
                    .map(str::to_uppercase);
                if !http_api && (path.is_none() || method.is_none()) {
                    return Err(invalid_event(
                        function_id,
                        event_name,
                        "Event is missing key 'Path' or 'Method'.",
                    ));
                }
                let method = method.unwrap_or_else(|| "ANY".to_string());
                let method = if method == "X-AMAZON-APIGATEWAY-ANY-METHOD" {
                    "ANY".to_string()
                } else {
                    method
                };
                ctx.api_routes.push(ApiRoute {
                    target: target.clone(),
                    id_base: id_base.clone(),
                    event_name: event_name.clone(),
                    api_id,
                    path,
                    method,
                    http_api,
                    auth: props.get("Auth").and_then(Value::as_object).cloned(),
                    payload_format_version: props.get("PayloadFormatVersion").cloned(),
                    timeout_in_millis: props.get("TimeoutInMillis").cloned(),
                    integration: None,
                });
            }
            other => {
                return Err(invalid_event(
                    function_id,
                    event_name,
                    &format!("Event type '{other}' is not supported."),
                ));
            }
        }
    }

    Ok(extras)
}

/// The CodeDeploy resources a `DeploymentPreference` adds, plus the alias's
/// `UpdatePolicy`.
type DeploymentExpansion = (Vec<(String, Value)>, Option<Value>);

/// `DeploymentPreference` -> the shared `ServerlessDeploymentApplication`
/// (CodeDeploy, Lambda compute platform), its service role, and a
/// `<Function>DeploymentGroup` running the requested deployment config, plus
/// the `CodeDeployLambdaAliasUpdate` UpdatePolicy SAM puts on the alias.
/// Returns no resources when the preference is disabled.
///
/// The alias is moved to the new version as part of the stack update (the
/// effect of an all-at-once deployment); the gradual traffic shift and
/// Pre/PostTraffic hooks a canary/linear config runs are not simulated.
fn deployment_preference(function_id: &str, pref: &Value) -> Result<DeploymentExpansion, String> {
    let enabled = pref.get("Enabled").and_then(Value::as_bool).unwrap_or(true);
    if !enabled {
        return Ok((Vec::new(), None));
    }
    let ty = pref.get("Type").and_then(Value::as_str).ok_or_else(|| {
        invalid_resource(
            function_id,
            "'DeploymentPreference' is missing required Property 'Type'",
        )
    })?;
    let config_name = if ty.starts_with("CodeDeployDefault.") {
        ty.to_string()
    } else {
        format!("CodeDeployDefault.Lambda{ty}")
    };
    let app_id = "ServerlessDeploymentApplication";
    let mut out = vec![(
        app_id.to_string(),
        json!({
            "Type": "AWS::CodeDeploy::Application",
            "Properties": { "ComputePlatform": "Lambda" }
        }),
    )];
    let role_arn = match pref.get("Role") {
        Some(role) => role.clone(),
        None => {
            out.push((
                "CodeDeployServiceRole".to_string(),
                json!({
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [{
                                "Effect": "Allow",
                                "Principal": { "Service": ["codedeploy.amazonaws.com"] },
                                "Action": ["sts:AssumeRole"]
                            }]
                        },
                        "ManagedPolicyArns": [
                            managed_policy_arn("service-role/AWSCodeDeployRoleForLambda")
                        ]
                    }
                }),
            ));
            json!({ "Fn::GetAtt": ["CodeDeployServiceRole", "Arn"] })
        }
    };
    let group_id = format!("{function_id}DeploymentGroup");
    let mut group = json!({
        "ApplicationName": { "Ref": app_id },
        "DeploymentConfigName": config_name,
        "DeploymentStyle": {
            "DeploymentType": "BLUE_GREEN",
            "DeploymentOption": "WITH_TRAFFIC_CONTROL"
        },
        "ServiceRoleArn": role_arn,
        "AutoRollbackConfiguration": {
            "Enabled": true,
            "Events": ["DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_ALARM", "DEPLOYMENT_STOP_ON_REQUEST"]
        }
    });
    if let Some(alarms) = pref.get("Alarms").and_then(Value::as_array) {
        group["AlarmConfiguration"] = json!({
            "Enabled": true,
            "Alarms": alarms.iter().map(|a| json!({ "Name": a })).collect::<Vec<_>>(),
        });
    }
    if let Some(triggers) = pref.get("TriggerConfigurations") {
        group["TriggerConfigurations"] = triggers.clone();
    }
    out.push((
        group_id.clone(),
        json!({ "Type": "AWS::CodeDeploy::DeploymentGroup", "Properties": group }),
    ));
    let update_policy = json!({
        "CodeDeployLambdaAliasUpdate": {
            "ApplicationName": { "Ref": app_id },
            "DeploymentGroupName": { "Ref": group_id },
        }
    });
    Ok((out, Some(update_policy)))
}

/// Build an `AWS::IAM::Role` from a function's `Policies`. The trust policy
/// always allows `lambda.amazonaws.com` to assume it. `Policies` entries that
/// are managed-policy names/ARNs become `ManagedPolicyArns`; inline statement
/// documents become inline `Policies`. SAM policy *templates* (object with a
/// single template key like `DynamoDBCrudPolicy`) are not expanded here — the
/// role is still created (assume-role works) so the function is no longer
/// role-less.
fn build_execution_role(policies: Option<&Value>) -> Value {
    let mut managed_arns: Vec<Value> = vec![managed_policy_arn(
        "service-role/AWSLambdaBasicExecutionRole",
    )];
    let mut inline: Vec<Value> = Vec::new();

    let mut handle = |p: &Value| {
        if let Some(name) = p.as_str() {
            managed_arns.push(managed_policy_arn(name));
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
/// `AmazonS3ReadOnlyAccess`) becomes the AWS-managed ARN in the stack's
/// partition; anything already in `arn:` form passes through.
fn managed_policy_arn(name: &str) -> Value {
    if name.starts_with("arn:") {
        json!(name)
    } else {
        json!({ "Fn::Sub": format!("arn:${{AWS::Partition}}:iam::aws:policy/{name}") })
    }
}

/// `Schedule` event -> `Events::Rule` (ScheduleExpression) targeting the
/// function + a `Lambda::Permission` allowing EventBridge to invoke it.
fn schedule_event(
    target: &FnTarget,
    id_base: &str,
    props: &Map<String, Value>,
) -> Vec<(String, Value)> {
    let function_id = &target.function_id;
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
            "Arn": target.arn.clone()
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
                target,
                "events.amazonaws.com",
                Some(json!({ "Fn::GetAtt": [rule_id, "Arn"] })),
            ),
        ),
    ]
}

/// SQS / DynamoDB / Kinesis / MSK / MQ / SelfManagedKafka / DocumentDB
/// event -> `Lambda::EventSourceMapping`.
fn event_source_mapping(
    target: &FnTarget,
    id_base: &str,
    event_type: &str,
    props: &Map<String, Value>,
) -> (String, Value) {
    let mut esm = json!({ "FunctionName": target.name_ref.clone() });
    // The source property name varies by event type.
    let source = match event_type {
        "SQS" => props.get("Queue"),
        "DynamoDB" | "Kinesis" | "MSK" => props.get("Stream"),
        "MQ" => props.get("Broker"),
        "DocumentDB" => props.get("Cluster"),
        _ => None,
    };
    if let Some(source) = source {
        esm["EventSourceArn"] = source.clone();
    }
    if event_type == "SelfManagedKafka" {
        esm["SelfManagedEventSource"] = json!({
            "Endpoints": {
                "KafkaBootstrapServers": props.get("KafkaBootstrapServers").cloned().unwrap_or_else(|| json!([]))
            }
        });
    }
    // SAM's Kafka events carry the consumer group and schema registry flat;
    // the mapping takes them in its source's Kafka config.
    let kafka_config = match event_type {
        "MSK" => Some("AmazonManagedKafkaEventSourceConfig"),
        "SelfManagedKafka" => Some("SelfManagedKafkaEventSourceConfig"),
        _ => None,
    };
    if let Some(config_key) = kafka_config {
        let mut cfg = Map::new();
        for key in ["ConsumerGroupId", "SchemaRegistryConfig"] {
            if let Some(v) = props.get(key) {
                cfg.insert(key.to_string(), v.clone());
            }
        }
        if !cfg.is_empty() {
            esm[config_key] = Value::Object(cfg);
        }
    }
    if event_type == "DocumentDB" {
        let mut cfg = Map::new();
        for key in ["DatabaseName", "CollectionName", "FullDocument"] {
            if let Some(v) = props.get(key) {
                cfg.insert(key.to_string(), v.clone());
            }
        }
        esm["DocumentDBEventSourceConfig"] = Value::Object(cfg);
    }
    for key in [
        "BatchSize",
        "Enabled",
        "FilterCriteria",
        "MaximumBatchingWindowInSeconds",
        "FunctionResponseTypes",
        "DestinationConfig",
        "MaximumRetryAttempts",
        "BisectBatchOnFunctionError",
        "MaximumRecordAgeInSeconds",
        "ParallelizationFactor",
        "TumblingWindowInSeconds",
        "StartingPositionTimestamp",
        "SourceAccessConfigurations",
        "Topics",
        "Queues",
        "KmsKeyArn",
        "MetricsConfig",
        "ScalingConfig",
        "ProvisionedPollerConfig",
        "LoggingConfig",
    ] {
        if let Some(v) = props.get(key) {
            esm[key] = v.clone();
        }
    }
    // Stream sources need a StartingPosition; SAM defaults to TRIM_HORIZON
    // for the stream types where the position is optional.
    if let Some(sp) = props.get("StartingPosition") {
        esm["StartingPosition"] = sp.clone();
    } else if matches!(event_type, "DynamoDB" | "Kinesis" | "MSK") {
        esm["StartingPosition"] = json!("TRIM_HORIZON");
    }
    (
        format!("{id_base}EventSourceMapping"),
        json!({ "Type": "AWS::Lambda::EventSourceMapping", "Properties": esm }),
    )
}

/// SNS event -> `SNS::Subscription` (protocol lambda) + invoke permission.
fn sns_event(target: &FnTarget, id_base: &str, props: &Map<String, Value>) -> Vec<(String, Value)> {
    let topic = props.get("Topic").cloned().unwrap_or(Value::Null);
    let mut sub_props = json!({
        "TopicArn": topic.clone(),
        "Protocol": "lambda",
        "Endpoint": target.arn.clone()
    });
    for key in [
        "FilterPolicy",
        "FilterPolicyScope",
        "Region",
        "RedrivePolicy",
    ] {
        if let Some(v) = props.get(key) {
            sub_props[key] = v.clone();
        }
    }
    vec![
        (
            format!("{id_base}Subscription"),
            json!({ "Type": "AWS::SNS::Subscription", "Properties": sub_props }),
        ),
        (
            format!("{id_base}Permission"),
            lambda_permission(target, "sns.amazonaws.com", Some(topic)),
        ),
    ]
}

/// EventBridgeRule / CloudWatchEvent event -> `Events::Rule` (EventPattern)
/// targeting the function + invoke permission.
fn eventbridge_event(
    target: &FnTarget,
    id_base: &str,
    props: &Map<String, Value>,
) -> Vec<(String, Value)> {
    let function_id = &target.function_id;
    let rule_id = format!("{id_base}Rule");
    let mut rule_props = json!({
        "Targets": [{
            "Id": format!("{function_id}Target"),
            "Arn": target.arn.clone()
        }]
    });
    if let Some(pattern) = props.get("Pattern").or_else(|| props.get("EventPattern")) {
        rule_props["EventPattern"] = pattern.clone();
    }
    if let Some(bus) = props.get("EventBusName") {
        rule_props["EventBusName"] = bus.clone();
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
                target,
                "events.amazonaws.com",
                Some(json!({ "Fn::GetAtt": [rule_id, "Arn"] })),
            ),
        ),
    ]
}

/// S3 event -> a `LambdaConfiguration` added to the referenced bucket's
/// `NotificationConfiguration` (applied after the function pass) + a
/// `Lambda::Permission` for `s3.amazonaws.com`, which the bucket depends on.
/// SAM requires the bucket be an `AWS::S3::Bucket` in the same template.
fn s3_event(
    target: &FnTarget,
    event_name: &str,
    id_base: &str,
    props: &Map<String, Value>,
    ctx: &mut SamContext,
) -> Result<Vec<(String, Value)>, String> {
    let bucket_id = ref_id(props.get("Bucket"))
        .filter(|id| ctx.resource_type(id) == Some("AWS::S3::Bucket"))
        .ok_or_else(|| {
            invalid_event(
                &target.function_id,
                event_name,
                "S3 events must reference an S3 bucket in the same template.",
            )
        })?
        .to_string();
    let events: Vec<Value> = match props.get("Events") {
        Some(Value::String(s)) => vec![json!(s)],
        Some(Value::Array(a)) if !a.is_empty() => a.clone(),
        _ => {
            return Err(invalid_event(
                &target.function_id,
                event_name,
                "Missing required property 'Events'.",
            ))
        }
    };
    let permission_id = format!("{id_base}Permission");
    for event in events {
        let mut config = json!({ "Event": event, "Function": target.arn.clone() });
        if let Some(filter) = props.get("Filter") {
            config["Filter"] = filter.clone();
        }
        ctx.bucket_notifications
            .push((bucket_id.clone(), config, permission_id.clone()));
    }
    // The bucket depends on this permission, so it cannot reference the
    // bucket back; SAM scopes it by the bucket's literal name when it has
    // one, and always by the account.
    let source_arn = ctx
        .resources
        .get(&bucket_id)
        .and_then(|b| b.pointer("/Properties/BucketName"))
        .and_then(Value::as_str)
        .map(|name| json!({ "Fn::Sub": format!("arn:${{AWS::Partition}}:s3:::{name}") }));
    let mut perm = lambda_permission(target, "s3.amazonaws.com", source_arn);
    perm["Properties"]["SourceAccount"] = json!({ "Ref": "AWS::AccountId" });
    Ok(vec![(permission_id, perm)])
}

/// CloudWatchLogs event -> `Logs::SubscriptionFilter` delivering the log
/// group to the function + a permission for `logs.amazonaws.com`.
fn cloudwatch_logs_event(
    target: &FnTarget,
    event_name: &str,
    id_base: &str,
    props: &Map<String, Value>,
) -> Result<Vec<(String, Value)>, String> {
    let log_group = props.get("LogGroupName").cloned().ok_or_else(|| {
        invalid_event(
            &target.function_id,
            event_name,
            "Missing required property 'LogGroupName'.",
        )
    })?;
    let filter_pattern = props.get("FilterPattern").cloned().ok_or_else(|| {
        invalid_event(
            &target.function_id,
            event_name,
            "Missing required property 'FilterPattern'.",
        )
    })?;
    let permission_id = format!("{id_base}Permission");
    let source_arn = json!({
        "Fn::Sub": [
            "arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:${__LogGroupName__}:*",
            { "__LogGroupName__": log_group.clone() }
        ]
    });
    Ok(vec![
        (
            permission_id.clone(),
            lambda_permission(target, "logs.amazonaws.com", Some(source_arn)),
        ),
        (
            id_base.to_string(),
            json!({
                "Type": "AWS::Logs::SubscriptionFilter",
                "DependsOn": [permission_id],
                "Properties": {
                    "DestinationArn": target.arn.clone(),
                    "FilterPattern": filter_pattern,
                    "LogGroupName": log_group,
                }
            }),
        ),
    ])
}

/// The user-pool triggers a SAM `Cognito` event can set.
const COGNITO_TRIGGERS: &[&str] = &[
    "CreateAuthChallenge",
    "CustomMessage",
    "DefineAuthChallenge",
    "PostAuthentication",
    "PostConfirmation",
    "PreAuthentication",
    "PreSignUp",
    "PreTokenGeneration",
    "UserMigration",
    "VerifyAuthChallengeResponse",
];

/// Cognito event -> the function set as the referenced user pool's
/// `LambdaConfig` trigger(s) (applied after the function pass) + a
/// permission for `cognito-idp.amazonaws.com` scoped to the pool.
fn cognito_event(
    target: &FnTarget,
    event_name: &str,
    id_base: &str,
    props: &Map<String, Value>,
    ctx: &mut SamContext,
) -> Result<Vec<(String, Value)>, String> {
    let pool_id = ref_id(props.get("UserPool"))
        .filter(|id| ctx.resource_type(id) == Some("AWS::Cognito::UserPool"))
        .ok_or_else(|| {
            invalid_event(
                &target.function_id,
                event_name,
                "Cognito events must reference a Cognito UserPool in the same template.",
            )
        })?
        .to_string();
    let triggers: Vec<String> = match props.get("Trigger") {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    };
    if triggers.is_empty() {
        return Err(invalid_event(
            &target.function_id,
            event_name,
            "Missing required property 'Trigger'.",
        ));
    }
    for trigger in triggers {
        if !COGNITO_TRIGGERS.contains(&trigger.as_str()) {
            return Err(invalid_event(
                &target.function_id,
                event_name,
                &format!("Cognito trigger '{trigger}' is not supported."),
            ));
        }
        ctx.user_pool_triggers.push((
            pool_id.clone(),
            trigger,
            target.arn.clone(),
            event_name.to_string(),
        ));
    }
    Ok(vec![(
        format!("{id_base}Permission"),
        lambda_permission(
            target,
            "cognito-idp.amazonaws.com",
            Some(json!({ "Fn::GetAtt": [pool_id, "Arn"] })),
        ),
    )])
}

/// IoTRule event -> `IoT::TopicRule` whose action invokes the function + a
/// permission for `iot.amazonaws.com` scoped to the rule.
fn iot_rule_event(
    target: &FnTarget,
    event_name: &str,
    id_base: &str,
    props: &Map<String, Value>,
) -> Result<Vec<(String, Value)>, String> {
    let sql = props.get("Sql").cloned().ok_or_else(|| {
        invalid_event(
            &target.function_id,
            event_name,
            "Missing required property 'Sql'.",
        )
    })?;
    let mut payload = json!({
        "Sql": sql,
        "RuleDisabled": false,
        "Actions": [{ "Lambda": { "FunctionArn": target.arn.clone() } }],
    });
    if let Some(v) = props.get("AwsIotSqlVersion") {
        payload["AwsIotSqlVersion"] = v.clone();
    }
    let mut perm = lambda_permission(
        target,
        "iot.amazonaws.com",
        Some(json!({
            "Fn::Sub": format!("arn:${{AWS::Partition}}:iot:${{AWS::Region}}:${{AWS::AccountId}}:rule/${{{id_base}}}")
        })),
    );
    perm["Properties"]["SourceAccount"] = json!({ "Ref": "AWS::AccountId" });
    Ok(vec![
        (
            id_base.to_string(),
            json!({ "Type": "AWS::IoT::TopicRule", "Properties": { "TopicRulePayload": payload } }),
        ),
        (format!("{id_base}Permission"), perm),
    ])
}

/// Build an `AWS::Lambda::Permission` granting `principal` invoke on the
/// function (or its alias), scoped to `source_arn` when given.
pub(super) fn lambda_permission(
    target: &FnTarget,
    principal: &str,
    source_arn: Option<Value>,
) -> Value {
    let mut props = json!({
        "FunctionName": target.name_ref.clone(),
        "Action": "lambda:InvokeFunction",
        "Principal": principal,
    });
    if let Some(arn) = source_arn {
        props["SourceArn"] = arn;
    }
    json!({ "Type": "AWS::Lambda::Permission", "Properties": props })
}

/// Apply the edits events recorded against other resources: a bucket's
/// `NotificationConfiguration.LambdaConfigurations` (the bucket then depends
/// on each permission, so S3 may invoke the function from the start) and a
/// user pool's `LambdaConfig`.
pub(super) fn apply_cross_resource_edits(
    ctx: &SamContext,
    resources: &mut Map<String, Value>,
) -> Result<(), String> {
    for (bucket_id, config, permission_id) in &ctx.bucket_notifications {
        let bucket = resources
            .get_mut(bucket_id)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                format!("S3 event references bucket {bucket_id} that is not in the template")
            })?;
        let props = bucket.entry("Properties").or_insert_with(|| json!({}));
        let configs = props
            .as_object_mut()
            .ok_or("bucket Properties must be an object")?
            .entry("NotificationConfiguration")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or("NotificationConfiguration must be an object")?
            .entry("LambdaConfigurations")
            .or_insert_with(|| json!([]));
        configs
            .as_array_mut()
            .ok_or("LambdaConfigurations must be a list")?
            .push(config.clone());
        let depends = bucket.entry("DependsOn").or_insert_with(|| json!([]));
        if let Value::String(s) = depends {
            *depends = json!([s.clone()]);
        }
        if let Some(list) = depends.as_array_mut() {
            if !list.iter().any(|d| d.as_str() == Some(permission_id)) {
                list.push(json!(permission_id));
            }
        }
    }
    for (pool_id, trigger, arn, event_name) in &ctx.user_pool_triggers {
        let lambda_config = resources
            .get_mut(pool_id)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                format!("Cognito event references user pool {pool_id} that is not in the template")
            })?
            .entry("Properties")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or("user pool Properties must be an object")?
            .entry("LambdaConfig")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or("LambdaConfig must be an object")?;
        if lambda_config.contains_key(trigger) {
            return Err(format!(
                "Transform AWS::Serverless-2016-10-31 failed: Resource with id [{pool_id}] is invalid. \
                 Cognito trigger \"{trigger}\" defined multiple times (event {event_name})."
            ));
        }
        lambda_config.insert(trigger.clone(), arn.clone());
    }
    Ok(())
}

/// Resolve SAM's `Ref: MyFunction.Alias` / `MyFunction.Version` /
/// `MyApi.Stage` / `MyApi.Deployment` references (in `Ref` and in `Fn::Sub`
/// `${...}` tokens) to the generated resources' logical ids.
pub(super) fn rewrite_sam_refs(value: &mut Value, aliases: &[(String, String)]) {
    if aliases.is_empty() {
        return;
    }
    match value {
        Value::Object(map) => {
            if map.len() == 1 {
                if let Some(Value::String(target)) = map.get_mut("Ref") {
                    if let Some((_, id)) = aliases.iter().find(|(k, _)| k == target) {
                        *target = id.clone();
                    }
                    return;
                }
                if let Some(sub) = map.get_mut("Fn::Sub") {
                    match sub {
                        Value::String(s) => *s = rewrite_sub_tokens(s, aliases),
                        Value::Array(parts) => {
                            if let Some(Value::String(s)) = parts.get_mut(0) {
                                *s = rewrite_sub_tokens(s, aliases);
                            }
                            for part in parts.iter_mut().skip(1) {
                                rewrite_sam_refs(part, aliases);
                            }
                        }
                        _ => {}
                    }
                    return;
                }
            }
            for v in map.values_mut() {
                rewrite_sam_refs(v, aliases);
            }
        }
        Value::Array(arr) => arr.iter_mut().for_each(|v| rewrite_sam_refs(v, aliases)),
        _ => {}
    }
}

fn rewrite_sub_tokens(s: &str, aliases: &[(String, String)]) -> String {
    let mut out = s.to_string();
    for (from, to) in aliases {
        out = out.replace(&format!("${{{from}}}"), &format!("${{{to}}}"));
    }
    out
}

/// Strip a path/method into a logical-id-safe token (alphanumeric only).
pub(super) fn sanitize(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

/// The AWS_PROXY integration URI for a Lambda target, resolved at provision
/// time via `Fn::Sub` over the function's (or alias's) ARN.
pub(super) fn lambda_integration_uri(target: &FnTarget) -> Value {
    json!({
        "Fn::Sub": format!(
            "arn:${{AWS::Partition}}:apigateway:${{AWS::Region}}:lambda:path/2015-03-31/functions/{}/invocations",
            target.sub_token
        )
    })
}

// ============================================================================
// AWS::Serverless::StateMachine `Events` expansion
// ============================================================================

/// Expand an `AWS::Serverless::StateMachine`'s `Events` into native trigger
/// resources, mirroring [`expand_function_extras`] for functions.
///
/// - `Schedule` / `ScheduleV2` and `EventBridgeRule` / `CloudWatchEvent`
///   become `AWS::Events::Rule`s whose target is the state machine's ARN, with
///   a shared execution role granting `states:StartExecution`.
/// - `Api` adds its path + method to the API it names (`RestApiId`, else the
///   implicit `ServerlessRestApi`, shared with function `Api` events and
///   deployed to its stage) with an AWS service integration that calls
///   `StartExecution`, plus the role API Gateway assumes to do so.
///
/// Returns the extra native resources keyed by logical id; API routes are
/// recorded in `ctx` for the API expansion.
pub(super) fn expand_state_machine_events(
    state_machine_id: &str,
    events: &Map<String, Value>,
    ctx: &mut SamContext,
) -> Result<Vec<(String, Value)>, String> {
    let mut extras: Vec<(String, Value)> = Vec::new();
    let start_role_id = format!("{state_machine_id}EventsRole");
    let mut needs_start_role = false;

    for (event_name, event) in events {
        let Some(event_obj) = event.as_object() else {
            continue;
        };
        let event_type = event_obj.get("Type").and_then(|v| v.as_str()).unwrap_or("");
        let props = event_obj
            .get("Properties")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let id_base = format!("{state_machine_id}{event_name}");
        match event_type {
            "Schedule" | "ScheduleV2" => {
                needs_start_role = true;
                extras.push(sfn_schedule_rule(
                    state_machine_id,
                    &start_role_id,
                    &id_base,
                    &props,
                ));
            }
            "EventBridgeRule" | "CloudWatchEvent" => {
                needs_start_role = true;
                extras.push(sfn_eventbridge_rule(
                    state_machine_id,
                    &start_role_id,
                    &id_base,
                    &props,
                ));
            }
            "Api" => {
                extras.push(sfn_api_route(
                    state_machine_id,
                    event_name,
                    &id_base,
                    &props,
                    ctx,
                )?);
            }
            other => {
                return Err(invalid_event(
                    state_machine_id,
                    event_name,
                    &format!("Event type '{other}' is not supported."),
                ));
            }
        }
    }

    if needs_start_role {
        extras.insert(
            0,
            (
                start_role_id.clone(),
                start_execution_role(state_machine_id, "events.amazonaws.com"),
            ),
        );
    }
    Ok(extras)
}

/// An IAM role that `assume_principal` can assume to call
/// `states:StartExecution` on the given state machine.
fn start_execution_role(state_machine_id: &str, assume_principal: &str) -> Value {
    json!({
        "Type": "AWS::IAM::Role",
        "Properties": {
            "AssumeRolePolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": { "Service": assume_principal },
                    "Action": "sts:AssumeRole"
                }]
            },
            "Policies": [{
                "PolicyName": "StartExecution",
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": "states:StartExecution",
                        "Resource": { "Ref": state_machine_id }
                    }]
                }
            }]
        }
    })
}

/// `Schedule`/`ScheduleV2` state-machine event -> `Events::Rule` whose target
/// is the state machine, invoked through the shared StartExecution role.
fn sfn_schedule_rule(
    state_machine_id: &str,
    role_id: &str,
    id_base: &str,
    props: &Map<String, Value>,
) -> (String, Value) {
    let schedule = props
        .get("Schedule")
        .or_else(|| props.get("ScheduleExpression"))
        .cloned()
        .unwrap_or(json!("rate(1 day)"));
    let mut rule_props = json!({
        "ScheduleExpression": schedule,
        "State": props.get("Enabled").map(|e| if e.as_bool() == Some(false) { json!("DISABLED") } else { json!("ENABLED") }).unwrap_or(json!("ENABLED")),
        "Targets": [{
            "Id": format!("{state_machine_id}Target"),
            "Arn": { "Fn::GetAtt": [state_machine_id, "Arn"] },
            "RoleArn": { "Fn::GetAtt": [role_id, "Arn"] }
        }]
    });
    if let Some(name) = props.get("Name") {
        rule_props["Name"] = name.clone();
    }
    if let Some(input) = props.get("Input") {
        rule_props["Targets"][0]["Input"] = input.clone();
    }
    (
        format!("{id_base}Rule"),
        json!({ "Type": "AWS::Events::Rule", "Properties": rule_props }),
    )
}

/// `EventBridgeRule`/`CloudWatchEvent` state-machine event -> `Events::Rule`
/// (EventPattern) targeting the state machine via the StartExecution role.
fn sfn_eventbridge_rule(
    state_machine_id: &str,
    role_id: &str,
    id_base: &str,
    props: &Map<String, Value>,
) -> (String, Value) {
    let mut rule_props = json!({
        "Targets": [{
            "Id": format!("{state_machine_id}Target"),
            "Arn": { "Fn::GetAtt": [state_machine_id, "Arn"] },
            "RoleArn": { "Fn::GetAtt": [role_id, "Arn"] }
        }]
    });
    if let Some(pattern) = props.get("Pattern").or_else(|| props.get("EventPattern")) {
        rule_props["EventPattern"] = pattern.clone();
    }
    if let Some(bus) = props.get("EventBusName") {
        rule_props["EventBusName"] = bus.clone();
    }
    if let Some(input) = props.get("Input") {
        rule_props["Targets"][0]["Input"] = input.clone();
    }
    (
        format!("{id_base}Rule"),
        json!({ "Type": "AWS::Events::Rule", "Properties": rule_props }),
    )
}

/// A state machine `Api` event: a route on the API it names whose
/// integration calls `StartExecution` with the request body as the input
/// (SAM's request template), through a role API Gateway assumes. Returns the
/// role; the route is recorded in `ctx` for the API expansion.
fn sfn_api_route(
    state_machine_id: &str,
    event_name: &str,
    id_base: &str,
    props: &Map<String, Value>,
    ctx: &mut SamContext,
) -> Result<(String, Value), String> {
    let api_id = match props.get("RestApiId") {
        None => "ServerlessRestApi".to_string(),
        Some(v) => ref_id(Some(v))
            .filter(|id| ctx.resource_type(id) == Some("AWS::Serverless::Api"))
            .ok_or_else(|| {
                invalid_event(
                    state_machine_id,
                    event_name,
                    "RestApiId must be a valid reference to an 'AWS::Serverless::Api' resource in same template.",
                )
            })?
            .to_string(),
    };
    let (Some(path), Some(method)) = (
        props.get("Path").and_then(|v| v.as_str()),
        props.get("Method").and_then(|v| v.as_str()),
    ) else {
        return Err(invalid_event(
            state_machine_id,
            event_name,
            "Event is missing key 'Path' or 'Method'.",
        ));
    };
    let role_id = format!("{id_base}Role");
    let template = format!(
        "{{\"input\": \"$util.escapeJavaScript($input.json('$'))\", \"stateMachineArn\": \"${{{state_machine_id}}}\"}}"
    );
    let integration = json!({
        "type": "aws",
        "httpMethod": "POST",
        "uri": { "Fn::Sub": "arn:${AWS::Partition}:apigateway:${AWS::Region}:states:action/StartExecution" },
        "credentials": { "Fn::GetAtt": [role_id, "Arn"] },
        "requestTemplates": { "application/json": { "Fn::Sub": template } },
        "responses": {
            "200": { "statusCode": "200" },
            "400": { "statusCode": "400" }
        }
    });
    let method = method.to_uppercase();
    ctx.api_routes.push(ApiRoute {
        target: FnTarget::function(state_machine_id),
        id_base: id_base.to_string(),
        event_name: event_name.to_string(),
        api_id,
        path: Some(path.to_string()),
        method: if method == "X-AMAZON-APIGATEWAY-ANY-METHOD" {
            "ANY".to_string()
        } else {
            method
        },
        http_api: false,
        auth: props.get("Auth").and_then(Value::as_object).cloned(),
        payload_format_version: None,
        timeout_in_millis: None,
        integration: Some(integration),
    });
    Ok((
        role_id,
        start_execution_role(state_machine_id, "apigateway.amazonaws.com"),
    ))
}

// ============================================================================
// AWS::Serverless::Connector expansion
// ============================================================================

/// Expand an `AWS::Serverless::Connector` into the IAM policy that grants the
/// Source's role the requested Read/Write actions on the Destination.
///
/// SAM connectors are IAM sugar: `{Source, Destination, Permissions}` becomes a
/// policy on the source's role scoped to the destination. This implements the
/// common source (Lambda function) -> destination (DynamoDB table, SQS queue,
/// SNS topic, S3 bucket, Lambda function) pairs. The policy is attached to the
/// source's implicit execution role (`<SourceId>Role`, the role the SAM
/// function transform synthesizes) unless the source *is* an IAM role.
/// `resources` is the original resource set, used to resolve a
/// Source/Destination type that the connector doesn't state inline.
pub(super) fn expand_connector(
    connector_id: &str,
    properties: &Value,
    resources: &Map<String, Value>,
) -> Vec<(String, Value)> {
    let Some(props) = properties.as_object() else {
        return Vec::new();
    };
    let source = props.get("Source").and_then(|v| v.as_object());
    let dest = props.get("Destination").and_then(|v| v.as_object());
    let (Some(source), Some(dest)) = (source, dest) else {
        return Vec::new();
    };

    let dest_id = dest.get("Id").and_then(|v| v.as_str());
    let Some(dest_id) = dest_id else {
        return Vec::new();
    };
    let dest_type = resolve_ref_type(dest, dest_id, resources);

    // Permissions: ["Read", "Write"] (default to both when omitted).
    let perms: Vec<String> = props
        .get("Permissions")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|p| p.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_else(|| vec!["Read".to_string(), "Write".to_string()]);
    let want_read = perms.iter().any(|p| p.eq_ignore_ascii_case("Read"));
    let want_write = perms.iter().any(|p| p.eq_ignore_ascii_case("Write"));

    let Some((actions, resource_arns)) =
        connector_actions(&dest_type, dest_id, want_read, want_write)
    else {
        // Unknown/unsupported destination pairing: nothing to synthesize.
        return Vec::new();
    };

    // Which role does the policy attach to? If the source is itself an IAM
    // role, attach directly; otherwise the source's implicit execution role.
    let Some(source_id) = source.get("Id").and_then(|v| v.as_str()) else {
        return Vec::new();
    };
    let source_type = resolve_ref_type(source, source_id, resources);
    let role_ref = if source_type == "AWS::IAM::Role" {
        json!({ "Ref": source_id })
    } else {
        json!({ "Ref": format!("{source_id}Role") })
    };

    let policy = json!({
        "Type": "AWS::IAM::Policy",
        "Properties": {
            "PolicyName": format!("{connector_id}Policy"),
            "Roles": [role_ref],
            "PolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": actions,
                    "Resource": resource_arns
                }]
            }
        }
    });

    vec![(format!("{connector_id}Policy"), policy)]
}

/// Resolve the AWS resource type of a Source/Destination reference: prefer an
/// inline `Type`, else look the id up in the template and map SAM sugar to its
/// native type.
fn resolve_ref_type(
    reference: &Map<String, Value>,
    id: &str,
    resources: &Map<String, Value>,
) -> String {
    if let Some(t) = reference.get("Type").and_then(|v| v.as_str()) {
        return t.to_string();
    }
    let raw = resources
        .get(id)
        .and_then(|r| r.get("Type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match raw {
        "AWS::Serverless::Function" => "AWS::Lambda::Function".to_string(),
        "AWS::Serverless::SimpleTable" => "AWS::DynamoDB::Table".to_string(),
        "AWS::Serverless::StateMachine" => "AWS::StepFunctions::StateMachine".to_string(),
        other => other.to_string(),
    }
}

/// Build the `(actions, resource_arns)` a connector policy grants for a given
/// destination type and Read/Write selection. Returns `None` for unsupported
/// destination types.
fn connector_actions(
    dest_type: &str,
    dest_id: &str,
    read: bool,
    write: bool,
) -> Option<(Vec<Value>, Vec<Value>)> {
    let mut actions: Vec<Value> = Vec::new();
    let arn = json!({ "Fn::GetAtt": [dest_id, "Arn"] });
    let arns: Vec<Value> = match dest_type {
        "AWS::DynamoDB::Table" => {
            if read {
                for a in [
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:BatchGetItem",
                    "dynamodb:ConditionCheckItem",
                    "dynamodb:DescribeTable",
                ] {
                    actions.push(json!(a));
                }
            }
            if write {
                for a in [
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:BatchWriteItem",
                ] {
                    actions.push(json!(a));
                }
            }
            vec![
                arn.clone(),
                json!({ "Fn::Sub": [ "${Arn}/index/*", { "Arn": arn } ] }),
            ]
        }
        "AWS::SQS::Queue" => {
            if read {
                for a in [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                ] {
                    actions.push(json!(a));
                }
            }
            if write {
                for a in [
                    "sqs:SendMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                ] {
                    actions.push(json!(a));
                }
            }
            vec![arn]
        }
        "AWS::SNS::Topic" => {
            if read {
                for a in ["sns:GetTopicAttributes", "sns:ListSubscriptionsByTopic"] {
                    actions.push(json!(a));
                }
            }
            if write {
                actions.push(json!("sns:Publish"));
            }
            // SNS `Ref` resolves to the topic ARN.
            vec![json!({ "Ref": dest_id })]
        }
        "AWS::S3::Bucket" => {
            if read {
                for a in [
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ] {
                    actions.push(json!(a));
                }
            }
            if write {
                for a in ["s3:PutObject", "s3:DeleteObject"] {
                    actions.push(json!(a));
                }
            }
            vec![
                arn.clone(),
                json!({ "Fn::Sub": [ "${Arn}/*", { "Arn": arn } ] }),
            ]
        }
        "AWS::Lambda::Function" => {
            // Invoking a function is a "write" in connector terms.
            for a in ["lambda:InvokeFunction", "lambda:InvokeAsync"] {
                actions.push(json!(a));
            }
            vec![arn]
        }
        "AWS::StepFunctions::StateMachine" => {
            if write {
                actions.push(json!("states:StartExecution"));
            }
            if read {
                for a in ["states:DescribeExecution", "states:DescribeStateMachine"] {
                    actions.push(json!(a));
                }
            }
            vec![json!({ "Ref": dest_id })]
        }
        _ => return None,
    };
    if actions.is_empty() {
        return None;
    }
    Some((actions, arns))
}

// ============================================================================
// AWS::Serverless::Application expansion
// ============================================================================

/// Expand an `AWS::Serverless::Application` into a native
/// `AWS::CloudFormation::Stack` (nested stack) pointing at the referenced
/// template, carrying `Parameters` through. `Location` may be a template URL /
/// `s3://` path (string), an object with `{Bucket, Key, Version}` or
/// `{TemplateURL}`, or a SAR reference (`{ApplicationId, SemanticVersion}`);
/// each is mapped onto the nested stack's `TemplateURL` so the resource has a
/// real backing instead of being recorded as a no-backing phantom.
pub(super) fn expand_application(properties: &Value, resource_obj: &Map<String, Value>) -> Value {
    let props = properties.as_object().cloned().unwrap_or_default();
    let template_url = match props.get("Location") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Object(loc)) => {
            if let Some(url) = loc.get("TemplateURL").and_then(|v| v.as_str()) {
                url.to_string()
            } else if let (Some(bucket), Some(key)) = (
                loc.get("Bucket").and_then(|v| v.as_str()),
                loc.get("Key").and_then(|v| v.as_str()),
            ) {
                format!("s3://{bucket}/{key}")
            } else if let Some(app_id) = loc.get("ApplicationId").and_then(|v| v.as_str()) {
                // SAR reference we can't resolve to a template body; still
                // record it as a real nested stack keyed by the SAR app id +
                // version so the resource isn't a phantom.
                match loc.get("SemanticVersion").and_then(|v| v.as_str()) {
                    Some(ver) => format!("{app_id}/{ver}"),
                    None => app_id.to_string(),
                }
            } else {
                String::new()
            }
        }
        _ => String::new(),
    };

    let mut stack_props = serde_json::Map::new();
    stack_props.insert("TemplateURL".to_string(), json!(template_url));
    if let Some(params) = props.get("Parameters") {
        stack_props.insert("Parameters".to_string(), params.clone());
    }
    if let Some(tags) = props.get("Tags") {
        stack_props.insert("Tags".to_string(), tags.clone());
    }
    if let Some(notif) = props.get("NotificationARNs") {
        stack_props.insert("NotificationARNs".to_string(), notif.clone());
    }
    if let Some(timeout) = props.get("TimeoutInMinutes") {
        stack_props.insert("TimeoutInMinutes".to_string(), timeout.clone());
    }

    let mut stack_resource = serde_json::Map::new();
    stack_resource.insert("Type".to_string(), json!("AWS::CloudFormation::Stack"));
    stack_resource.insert("Properties".to_string(), Value::Object(stack_props));
    for (k, v) in resource_obj {
        if k != "Type" && k != "Properties" {
            stack_resource.insert(k.clone(), v.clone());
        }
    }
    Value::Object(stack_resource)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthesized_api_arns_take_the_stack_partition() {
        let uri = lambda_integration_uri(&FnTarget::function("Fn"));
        assert!(
            uri["Fn::Sub"]
                .as_str()
                .unwrap()
                .starts_with("arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:"),
            "{uri}"
        );
    }

    #[test]
    fn managed_policy_arns_take_the_stack_partition() {
        assert_eq!(
            managed_policy_arn("AmazonS3ReadOnlyAccess"),
            json!({ "Fn::Sub": "arn:${AWS::Partition}:iam::aws:policy/AmazonS3ReadOnlyAccess" })
        );
        assert_eq!(
            managed_policy_arn("arn:aws-cn:iam::123456789012:policy/mine"),
            json!("arn:aws-cn:iam::123456789012:policy/mine")
        );
        let role = build_execution_role(None);
        assert_eq!(
            role["Properties"]["ManagedPolicyArns"][0],
            json!({ "Fn::Sub": "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" })
        );
    }

    #[test]
    fn policies_become_execution_role() {
        let mut props = serde_json::from_value::<Map<String, Value>>(json!({
            "Handler": "index.handler",
            "Policies": ["AmazonS3ReadOnlyAccess", {"Statement": [{"Effect":"Allow","Action":"logs:PutLogEvents","Resource":"*"}]}]
        }))
        .unwrap();
        let extras =
            expand_function_extras("MyFn", &mut props, &mut SamContext::default()).unwrap();
        // Role injected as GetAtt.
        assert_eq!(props["Role"], json!({"Fn::GetAtt": ["MyFnRole", "Arn"]}));
        assert!(props.get("Policies").is_none());
        let (rid, role) = extras.iter().find(|(id, _)| id == "MyFnRole").unwrap();
        assert_eq!(rid, "MyFnRole");
        let arns = role["Properties"]["ManagedPolicyArns"].as_array().unwrap();
        assert!(arns.iter().any(
            |a| a["Fn::Sub"] == "arn:${AWS::Partition}:iam::aws:policy/AmazonS3ReadOnlyAccess"
        ));
        assert!(role["Properties"]["Policies"].as_array().unwrap().len() == 1);
    }

    #[test]
    fn explicit_role_is_kept() {
        let mut props = serde_json::from_value::<Map<String, Value>>(json!({
            "Role": "arn:aws:iam::123456789012:role/explicit",
            "Policies": ["AmazonS3ReadOnlyAccess"]
        }))
        .unwrap();
        let extras =
            expand_function_extras("MyFn", &mut props, &mut SamContext::default()).unwrap();
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
        let extras =
            expand_function_extras("MyFn", &mut props, &mut SamContext::default()).unwrap();
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
        let extras =
            expand_function_extras("MyFn", &mut props, &mut SamContext::default()).unwrap();
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
