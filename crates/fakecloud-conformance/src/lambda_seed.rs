//! Lambda's per-operation seed: the function every function-scoped variant
//! addresses, plus whatever else the operation's identifiers name (a
//! published version, an alias, a layer version, an event source mapping,
//! a code signing config, ...), so a variant reaches the operation's own
//! behaviour instead of a `ResourceNotFoundException` for an identifier the
//! generators made up.

use std::collections::HashMap;

use serde_json::{json, Value};

use crate::{
    ExamplePins, OperationSeedContext, OperationSeedValues, SeedTeardown, TeardownStep,
    LAMBDA_SEED_AUTH, LAMBDA_SEED_ZIP,
};

/// The seeded layer, event-source queue and capacity provider (suffixed per
/// operation on an operator-supplied endpoint, see `SeedNames`).
pub(crate) const LAYER: &str = "test-conformance-layer";
pub(crate) const QUEUE: &str = "test-conformance-queue";
pub(crate) const CAPACITY_PROVIDER: &str = "test-conformance-capacity-provider";
/// The alias seeded on the function (to the published version). Aliases and
/// statements live on the function, so they need no per-operation names.
const ALIAS: &str = "conformance-alias";
/// The statement id of the seeded function and layer-version permissions.
const STATEMENT: &str = "conformance-statement";

const SQS_SEED_AUTH: &str =
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/sqs/aws4_request, \
                             SignedHeaders=host;x-amz-date, Signature=00";

/// Which of the function's qualifiers an operation's `Qualifier` names.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum QualifierTarget {
    /// The operation has no `Qualifier`, or none needs seeding.
    #[default]
    None,
    /// The seeded alias: operations that take a version or an alias, and
    /// those that only take an alias (function URLs).
    Alias,
    /// The published version: operations that take a version but no alias
    /// (DeleteFunction, runtime management, scaling), and DeleteFunction,
    /// which refuses a version an alias still points at, so it gets no alias.
    Version,
}

/// What one operation's seed sets up besides the function itself.
#[derive(Debug, Default, PartialEq)]
struct LambdaSeedState {
    /// Publish version 1 of the function.
    version: bool,
    /// Create [`ALIAS`] to the published version.
    alias: bool,
    qualifier: QualifierTarget,
    /// A function URL on the function (and on the alias when there is one).
    url_config: bool,
    /// An asynchronous-invocation config (also on the alias when seeded).
    event_invoke_config: bool,
    /// Provisioned concurrency on the alias.
    provisioned_concurrency: bool,
    /// Reserved concurrency on the function.
    reserved_concurrency: bool,
    /// A [`STATEMENT`] permission on the function (and on the alias).
    permission: bool,
    code_signing_config: bool,
    /// The code signing config is attached to the function.
    code_signing_attached: bool,
    /// A published layer version.
    layer: bool,
    /// A [`STATEMENT`] permission on the layer version.
    layer_permission: bool,
    /// An SQS queue to map events from.
    queue: bool,
    /// An event source mapping from the queue to the function.
    event_source_mapping: bool,
    capacity_provider: bool,
}

impl LambdaSeedState {
    fn for_operation(op_name: &str) -> Self {
        let qualifier = match op_name {
            "AddPermission"
            | "GetPolicy"
            | "RemovePermission"
            | "GetFunction"
            | "GetFunctionConfiguration"
            | "Invoke"
            | "InvokeWithResponseStream"
            | "CreateFunctionUrlConfig"
            | "GetFunctionUrlConfig"
            | "UpdateFunctionUrlConfig"
            | "DeleteFunctionUrlConfig"
            | "PutFunctionEventInvokeConfig"
            | "GetFunctionEventInvokeConfig"
            | "UpdateFunctionEventInvokeConfig"
            | "DeleteFunctionEventInvokeConfig"
            | "PutProvisionedConcurrencyConfig"
            | "GetProvisionedConcurrencyConfig"
            | "DeleteProvisionedConcurrencyConfig"
            | "ListDurableExecutionsByFunction" => QualifierTarget::Alias,
            "DeleteFunction"
            | "GetRuntimeManagementConfig"
            | "PutRuntimeManagementConfig"
            | "GetFunctionScalingConfig"
            | "PutFunctionScalingConfig" => QualifierTarget::Version,
            _ => QualifierTarget::None,
        };
        let url_config = matches!(
            op_name,
            "GetFunctionUrlConfig"
                | "UpdateFunctionUrlConfig"
                | "DeleteFunctionUrlConfig"
                | "ListFunctionUrlConfigs"
        );
        let event_invoke_config = matches!(
            op_name,
            "GetFunctionEventInvokeConfig"
                | "UpdateFunctionEventInvokeConfig"
                | "DeleteFunctionEventInvokeConfig"
                | "ListFunctionEventInvokeConfigs"
        );
        let provisioned_concurrency = matches!(
            op_name,
            "GetProvisionedConcurrencyConfig"
                | "DeleteProvisionedConcurrencyConfig"
                | "ListProvisionedConcurrencyConfigs"
        );
        let alias = qualifier == QualifierTarget::Alias
            || provisioned_concurrency
            || matches!(op_name, "GetAlias" | "UpdateAlias" | "DeleteAlias");
        let version = alias
            || qualifier == QualifierTarget::Version
            // CreateAlias / ListAliases name a version (`FunctionVersion`).
            || matches!(op_name, "CreateAlias" | "ListAliases");
        let code_signing_attached = matches!(
            op_name,
            "GetFunctionCodeSigningConfig"
                | "DeleteFunctionCodeSigningConfig"
                | "ListFunctionsByCodeSigningConfig"
        );
        let code_signing_config = code_signing_attached
            || matches!(
                op_name,
                "GetCodeSigningConfig"
                    | "UpdateCodeSigningConfig"
                    | "DeleteCodeSigningConfig"
                    | "PutFunctionCodeSigningConfig"
                    | "CreateFunction"
            );
        let layer_permission = matches!(
            op_name,
            "GetLayerVersionPolicy" | "RemoveLayerVersionPermission"
        );
        let layer = layer_permission
            || matches!(
                op_name,
                "AddLayerVersionPermission"
                    | "DeleteLayerVersion"
                    | "GetLayerVersion"
                    | "GetLayerVersionByArn"
                    | "ListLayerVersions"
                    // Their `Layers` attach the seeded layer version.
                    | "CreateFunction"
                    | "UpdateFunctionConfiguration"
            );
        let event_source_mapping = matches!(
            op_name,
            "GetEventSourceMapping"
                | "UpdateEventSourceMapping"
                | "DeleteEventSourceMapping"
                | "ListEventSourceMappings"
        );
        Self {
            version,
            alias,
            qualifier,
            url_config,
            event_invoke_config,
            provisioned_concurrency,
            reserved_concurrency: matches!(
                op_name,
                "GetFunctionConcurrency" | "DeleteFunctionConcurrency"
            ),
            permission: matches!(
                op_name,
                "GetPolicy" | "RemovePermission" | "GetResourcePolicy" | "DeleteResourcePolicy"
            ),
            code_signing_config,
            code_signing_attached,
            layer,
            layer_permission,
            queue: event_source_mapping || op_name == "CreateEventSourceMapping",
            event_source_mapping,
            capacity_provider: matches!(
                op_name,
                "GetCapacityProvider"
                    | "UpdateCapacityProvider"
                    | "DeleteCapacityProvider"
                    | "ListFunctionVersionsByCapacityProvider"
            ),
        }
    }
}

/// Whether `op_name` removes the resource its seed created (a Delete* or
/// Remove* operation), so that only the first variant to run would find it.
fn consumes_seed(op_name: &str) -> bool {
    op_name.starts_with("Delete") || op_name.starts_with("Remove")
}

/// The identifiers a seed issued, as the server returned them.
#[derive(Debug, Default)]
struct SeededIds {
    function_arn: String,
    code_sha256: Option<String>,
    version: Option<String>,
    layer_version: Option<i64>,
    layer_version_arn: Option<String>,
    queue_arn: Option<String>,
    esm_uuid: Option<String>,
    code_signing_config_arn: Option<String>,
}

/// The overrides an operation's variants are built with, from what was
/// seeded for it.
fn overrides_for(
    op_name: &str,
    state: &LambdaSeedState,
    function: &str,
    layer: &str,
    capacity_provider: &str,
    ids: &SeededIds,
) -> HashMap<String, Value> {
    let s = |v: &str| Value::String(v.to_string());
    let mut overrides = HashMap::new();
    if op_name != "CreateFunction" {
        overrides.insert("FunctionName".to_string(), s(function));
        // TagResource / UntagResource / ListTags and the resource-policy
        // operations address the function by ARN.
        overrides.insert("Resource".to_string(), s(&ids.function_arn));
        overrides.insert("ResourceArn".to_string(), s(&ids.function_arn));
    }
    match state.qualifier {
        QualifierTarget::Alias if state.alias => {
            overrides.insert("Qualifier".to_string(), s(ALIAS));
        }
        QualifierTarget::Version => {
            if let Some(v) = &ids.version {
                overrides.insert("Qualifier".to_string(), s(v));
            }
        }
        _ => {}
    }
    if matches!(op_name, "GetAlias" | "UpdateAlias" | "DeleteAlias") {
        overrides.insert("Name".to_string(), s(ALIAS));
    }
    if matches!(op_name, "CreateAlias" | "UpdateAlias" | "ListAliases") {
        if let Some(v) = &ids.version {
            overrides.insert("FunctionVersion".to_string(), s(v));
        }
    }
    if op_name == "PublishVersion" {
        // PublishVersion only publishes when `CodeSha256` matches $LATEST.
        if let Some(sha) = &ids.code_sha256 {
            overrides.insert("CodeSha256".to_string(), s(sha));
        }
    }
    if matches!(op_name, "RemovePermission" | "RemoveLayerVersionPermission") {
        overrides.insert("StatementId".to_string(), s(STATEMENT));
    }
    if state.layer {
        overrides.insert("LayerName".to_string(), s(layer));
        if let Some(n) = ids.layer_version {
            overrides.insert("VersionNumber".to_string(), json!(n));
        }
        if let Some(arn) = &ids.layer_version_arn {
            if op_name == "GetLayerVersionByArn" {
                overrides.insert("Arn".to_string(), s(arn));
            }
            overrides.insert("Layers".to_string(), json!([arn]));
        }
    }
    if let Some(uuid) = &ids.esm_uuid {
        overrides.insert("UUID".to_string(), s(uuid));
    }
    if let Some(arn) = &ids.queue_arn {
        overrides.insert("EventSourceArn".to_string(), s(arn));
    }
    if let Some(arn) = &ids.code_signing_config_arn {
        overrides.insert("CodeSigningConfigArn".to_string(), s(arn));
    }
    if state.capacity_provider {
        overrides.insert("CapacityProviderName".to_string(), s(capacity_provider));
    }
    overrides
}

/// Percent-encode a path segment (everything but RFC 3986 unreserved).
fn segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// One Lambda API call; the response body when it succeeded.
fn call(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Option<Value> {
    let mut req = client
        .request(method, format!("{endpoint}{path}"))
        .header("Authorization", LAMBDA_SEED_AUTH);
    if let Some(body) = body {
        req = req
            .header("Content-Type", "application/json")
            .body(body.to_string());
    }
    let resp = req.send().ok().filter(|r| r.status().is_success())?;
    let text = resp.text().ok()?;
    Some(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// An SQS query-protocol call; the response body.
fn sqs(client: &reqwest::blocking::Client, endpoint: &str, form: &str) -> Option<String> {
    client
        .post(endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Authorization", SQS_SEED_AUTH)
        .body(format!("{form}&Version=2012-11-05"))
        .send()
        .ok()
        .filter(|r| r.status().is_success())?
        .text()
        .ok()
}

/// Delete the SQS queue at `url`, best effort.
pub(crate) fn delete_queue(client: &reqwest::blocking::Client, endpoint: &str, url: &str) {
    let _ = sqs(
        client,
        endpoint,
        &format!("Action=DeleteQueue&QueueUrl={}", segment(url)),
    );
}

/// Delete every event source mapping of `function`, best effort.
pub(crate) fn delete_event_source_mappings(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    function: &str,
) {
    // Deleting shrinks the listing, so each round reads the first page again;
    // the bound stops a server that keeps answering with undeletable ones.
    for _ in 0..20 {
        let Some(listing) = call(
            client,
            endpoint,
            reqwest::Method::GET,
            &format!("/2015-03-31/event-source-mappings?FunctionName={function}&MaxItems=100"),
            None,
        ) else {
            return;
        };
        let uuids: Vec<&str> = listing["EventSourceMappings"]
            .as_array()
            .map(|a| a.iter().filter_map(|m| m["UUID"].as_str()).collect())
            .unwrap_or_default();
        if uuids.is_empty() {
            return;
        }
        for uuid in uuids {
            let _ = call(
                client,
                endpoint,
                reqwest::Method::DELETE,
                &format!("/2015-03-31/event-source-mappings/{uuid}"),
                None,
            );
        }
    }
}

/// Seed Lambda for `op_name`: the function (re-created before every
/// operation on a server the probe owns, so what one operation changed on it
/// -- configuration, aliases, versions, policy, tags, URLs, concurrency --
/// does not leak into the next) and whatever [`LambdaSeedState`] says the
/// operation's identifiers name. CreateFunction's variants keep their
/// generated function names.
///
/// Lambda is not reset between operations; instead everything the seed
/// created (the function, layer versions, event source mappings, code
/// signing configs, the queue, the capacity provider) is removed when the
/// operation is done, on any server, so no later operation sees it. The
/// documented example requests address the seeded resources; their output
/// diffs keep the documented names (see [`ExamplePins::Requests`]).
pub(crate) fn seed<'a>(
    client: &'a reqwest::blocking::Client,
    endpoint: &'a str,
    op_name: &str,
    ctx: &OperationSeedContext<'_>,
) -> OperationSeedValues<'a> {
    let Some(account) = ctx.account else {
        return OperationSeedValues::default();
    };
    let state = LambdaSeedState::for_operation(op_name);
    let names = &ctx.names;
    let function = names.function.as_str();
    let fn_path = format!("/2015-03-31/functions/{function}");
    // Removed in this order: the function's event source mappings first
    // (they outlive their function, as on AWS, and CreateEventSourceMapping's
    // variants create some of their own), then the function, what neither
    // depends on, and the code signing config the function may be attached
    // to last.
    let mut teardown_esm: Vec<TeardownStep> = Vec::new();
    if state.queue {
        teardown_esm.push(TeardownStep::EventSourceMappingsOf(function.to_string()));
    }
    let mut teardown_rest = Vec::new();
    // Best-effort throughout: a failed seed shows up as failing variants,
    // which is where it should be reported.
    if ctx.owns_server {
        // 404 for the first operation, and after one that deleted it.
        let _ = call(client, endpoint, reqwest::Method::DELETE, &fn_path, None);
    }
    let mut ids = SeededIds {
        function_arn: format!("arn:aws:lambda:us-east-1:{account}:function:{function}"),
        ..SeededIds::default()
    };

    if state.layer {
        let published = call(
            client,
            endpoint,
            reqwest::Method::POST,
            &format!("/2018-10-31/layers/{}/versions", names.layer),
            Some(json!({
                "Content": {"ZipFile": LAMBDA_SEED_ZIP},
                "CompatibleRuntimes": ["python3.12"],
            })),
        );
        ids.layer_version = published.as_ref().and_then(|v| v["Version"].as_i64());
        ids.layer_version_arn = published
            .as_ref()
            .and_then(|v| v["LayerVersionArn"].as_str())
            .map(str::to_string);
        if let Some(n) = ids.layer_version {
            let version_path = format!("/2018-10-31/layers/{}/versions/{n}", names.layer);
            if state.layer_permission {
                let _ = call(
                    client,
                    endpoint,
                    reqwest::Method::POST,
                    &format!("{version_path}/policy"),
                    Some(json!({
                        "StatementId": STATEMENT,
                        "Action": "lambda:GetLayerVersion",
                        "Principal": account,
                    })),
                );
            }
            teardown_rest.push(TeardownStep::Lambda(version_path));
        }
    }
    if state.code_signing_config {
        ids.code_signing_config_arn = call(
            client,
            endpoint,
            reqwest::Method::POST,
            "/2020-04-22/code-signing-configs",
            Some(json!({
                "AllowedPublishers": {"SigningProfileVersionArns": [format!(
                    "arn:aws:signer:us-east-1:{account}:/signing-profiles/conformance/AbCdEf1234"
                )]},
            })),
        )
        .and_then(|v| {
            v["CodeSigningConfig"]["CodeSigningConfigArn"]
                .as_str()
                .map(str::to_string)
        });
    }

    let created = call(
        client,
        endpoint,
        reqwest::Method::POST,
        "/2015-03-31/functions",
        Some(json!({
            "FunctionName": function,
            "Runtime": "python3.12",
            "Role": format!("arn:aws:iam::{account}:role/test-conformance-role"),
            "Handler": "index.handler",
            "Code": {"ZipFile": LAMBDA_SEED_ZIP},
        })),
    );
    if let Some(created) = &created {
        if let Some(arn) = created["FunctionArn"].as_str() {
            ids.function_arn = arn.to_string();
        }
        ids.code_sha256 = created["CodeSha256"].as_str().map(str::to_string);
    }
    if state.version {
        ids.version = call(
            client,
            endpoint,
            reqwest::Method::POST,
            &format!("{fn_path}/versions"),
            Some(json!({})),
        )
        .and_then(|v| v["Version"].as_str().map(str::to_string));
    }
    let alias = match (&ids.version, state.alias) {
        (Some(version), true) => call(
            client,
            endpoint,
            reqwest::Method::POST,
            &format!("{fn_path}/aliases"),
            Some(json!({"Name": ALIAS, "FunctionVersion": version})),
        )
        .map(|_| ALIAS),
        _ => None,
    };
    // The unqualified function, and the alias when there is one.
    let targets: Vec<String> = std::iter::once(String::new())
        .chain(alias.map(|a| format!("?Qualifier={a}")))
        .collect();
    if state.url_config {
        for q in &targets {
            let _ = call(
                client,
                endpoint,
                reqwest::Method::POST,
                &format!("/2021-10-31/functions/{function}/url{q}"),
                Some(json!({"AuthType": "AWS_IAM"})),
            );
        }
    }
    if state.event_invoke_config {
        for q in &targets {
            let _ = call(
                client,
                endpoint,
                reqwest::Method::PUT,
                &format!("/2019-09-25/functions/{function}/event-invoke-config{q}"),
                Some(json!({"MaximumRetryAttempts": 1})),
            );
        }
    }
    if state.permission {
        for q in &targets {
            let _ = call(
                client,
                endpoint,
                reqwest::Method::POST,
                &format!("{fn_path}/policy{q}"),
                Some(json!({
                    "StatementId": STATEMENT,
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                })),
            );
        }
    }
    if let (true, Some(alias)) = (state.provisioned_concurrency, alias) {
        let _ = call(
            client,
            endpoint,
            reqwest::Method::PUT,
            &format!("/2019-09-30/functions/{function}/provisioned-concurrency?Qualifier={alias}"),
            Some(json!({"ProvisionedConcurrentExecutions": 1})),
        );
    }
    if state.reserved_concurrency {
        let _ = call(
            client,
            endpoint,
            reqwest::Method::PUT,
            &format!("/2017-10-31/functions/{function}/concurrency"),
            Some(json!({"ReservedConcurrentExecutions": 5})),
        );
    }
    if let (true, Some(arn)) = (
        state.code_signing_attached,
        ids.code_signing_config_arn.as_deref(),
    ) {
        let _ = call(
            client,
            endpoint,
            reqwest::Method::PUT,
            &format!("/2020-06-30/functions/{function}/code-signing-config"),
            Some(json!({"CodeSigningConfigArn": arn})),
        );
    }
    if state.queue {
        let queue_url = sqs(
            client,
            endpoint,
            &format!("Action=CreateQueue&QueueName={}", names.queue),
        )
        .and_then(|body| crate::xml_text(&body, "QueueUrl"));
        if let Some(url) = queue_url {
            ids.queue_arn = Some(format!("arn:aws:sqs:us-east-1:{account}:{}", names.queue));
            if state.event_source_mapping {
                ids.esm_uuid = call(
                    client,
                    endpoint,
                    reqwest::Method::POST,
                    "/2015-03-31/event-source-mappings",
                    Some(json!({
                        "EventSourceArn": ids.queue_arn,
                        "FunctionName": function,
                        "Enabled": false,
                    })),
                )
                .and_then(|v| v["UUID"].as_str().map(str::to_string));
            }
            // UpdateEventSourceMapping's variants may point the seeded
            // mapping at another function, out of the listing's reach.
            if let Some(uuid) = &ids.esm_uuid {
                teardown_esm.push(TeardownStep::Lambda(format!(
                    "/2015-03-31/event-source-mappings/{uuid}"
                )));
            }
            teardown_rest.push(TeardownStep::SqsQueue(url));
        }
    }
    if state.capacity_provider {
        let created = call(
            client,
            endpoint,
            reqwest::Method::POST,
            "/2025-11-30/capacity-providers",
            Some(json!({
                "CapacityProviderName": names.capacity_provider,
                "VpcConfig": {
                    "SubnetIds": ["subnet-0123456789abcdef0"],
                    "SecurityGroupIds": ["sg-0123456789abcdef0"],
                },
                "PermissionsConfig": {
                    "CapacityProviderOperatorRoleArn":
                        format!("arn:aws:iam::{account}:role/test-conformance-role"),
                },
            })),
        );
        if created.is_some() {
            teardown_rest.push(TeardownStep::Lambda(format!(
                "/2025-11-30/capacity-providers/{}",
                names.capacity_provider
            )));
        }
    }

    let teardown: Vec<TeardownStep> = teardown_esm
        .into_iter()
        .chain(std::iter::once(TeardownStep::Lambda(fn_path.clone())))
        .chain(teardown_rest)
        .chain(ids.code_signing_config_arn.as_ref().map(|arn| {
            TeardownStep::Lambda(format!("/2020-04-22/code-signing-configs/{}", segment(arn)))
        }))
        .collect();

    let overrides = overrides_for(
        op_name,
        &state,
        function,
        &names.layer,
        &names.capacity_provider,
        &ids,
    );
    OperationSeedValues {
        overrides,
        pin_examples: ExamplePins::Requests,
        // A mapping needs a source: without one every variant is refused
        // before the operation runs.
        include: if op_name == "CreateEventSourceMapping" {
            vec!["EventSourceArn"]
        } else {
            Vec::new()
        },
        reseed_each_variant: consumes_seed(op_name),
        teardown: Some(SeedTeardown {
            client,
            endpoint,
            steps: teardown,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualifier_ops_get_a_version_and_the_alias_they_can_address() {
        let get = LambdaSeedState::for_operation("GetFunction");
        assert_eq!(get.qualifier, QualifierTarget::Alias);
        assert!(get.alias && get.version);
        // A function URL takes an alias, never a version.
        let url = LambdaSeedState::for_operation("CreateFunctionUrlConfig");
        assert_eq!(url.qualifier, QualifierTarget::Alias);
        // ...and Create must not find one already there.
        assert!(!url.url_config);
        // DeleteFunction refuses a version an alias points at.
        let delete = LambdaSeedState::for_operation("DeleteFunction");
        assert_eq!(delete.qualifier, QualifierTarget::Version);
        assert!(delete.version && !delete.alias);
        let alias = LambdaSeedState::for_operation("UpdateAlias");
        assert!(alias.alias && alias.version);
        assert!(LambdaSeedState::for_operation("CreateAlias").version);
        assert!(!LambdaSeedState::for_operation("CreateAlias").alias);
    }

    #[test]
    fn readers_and_deleters_find_what_they_address_creators_do_not() {
        assert!(LambdaSeedState::for_operation("GetFunctionUrlConfig").url_config);
        assert!(
            LambdaSeedState::for_operation("ListFunctionEventInvokeConfigs").event_invoke_config
        );
        assert!(
            !LambdaSeedState::for_operation("PutFunctionEventInvokeConfig").event_invoke_config
        );
        let pc = LambdaSeedState::for_operation("ListProvisionedConcurrencyConfigs");
        assert!(pc.provisioned_concurrency && pc.alias);
        assert!(
            !LambdaSeedState::for_operation("PutProvisionedConcurrencyConfig")
                .provisioned_concurrency
        );
        assert!(LambdaSeedState::for_operation("RemovePermission").permission);
        assert!(!LambdaSeedState::for_operation("AddPermission").permission);
        let esm = LambdaSeedState::for_operation("UpdateEventSourceMapping");
        assert!(esm.event_source_mapping && esm.queue);
        let create_esm = LambdaSeedState::for_operation("CreateEventSourceMapping");
        assert!(create_esm.queue && !create_esm.event_source_mapping);
        let lp = LambdaSeedState::for_operation("RemoveLayerVersionPermission");
        assert!(lp.layer && lp.layer_permission);
        assert!(!LambdaSeedState::for_operation("AddLayerVersionPermission").layer_permission);
        let csc = LambdaSeedState::for_operation("GetFunctionCodeSigningConfig");
        assert!(csc.code_signing_config && csc.code_signing_attached);
        let del_csc = LambdaSeedState::for_operation("DeleteCodeSigningConfig");
        assert!(del_csc.code_signing_config && !del_csc.code_signing_attached);
        assert!(LambdaSeedState::for_operation("GetCapacityProvider").capacity_provider);
        assert_eq!(
            LambdaSeedState::for_operation("ListFunctions"),
            LambdaSeedState::default()
        );
    }

    fn ids() -> SeededIds {
        SeededIds {
            function_arn: "arn:aws:lambda:us-east-1:123456789012:function:f".to_string(),
            code_sha256: Some("sha".to_string()),
            version: Some("1".to_string()),
            layer_version: Some(3),
            layer_version_arn: Some("arn:aws:lambda:us-east-1:123456789012:layer:l:3".to_string()),
            queue_arn: Some("arn:aws:sqs:us-east-1:123456789012:q".to_string()),
            esm_uuid: Some("uuid".to_string()),
            code_signing_config_arn: Some("csc-arn".to_string()),
        }
    }

    fn overrides(op: &str) -> HashMap<String, Value> {
        overrides_for(
            op,
            &LambdaSeedState::for_operation(op),
            "f",
            "l",
            "cp",
            &ids(),
        )
    }

    #[test]
    fn overrides_carry_the_seeded_ids_where_the_operation_names_them() {
        assert_eq!(overrides("Invoke")["Qualifier"], ALIAS);
        assert_eq!(overrides("DeleteFunction")["Qualifier"], "1");
        assert!(!overrides("ListFunctionUrlConfigs").contains_key("Qualifier"));
        assert_eq!(overrides("GetAlias")["Name"], ALIAS);
        // CreateAlias names a new alias: its `Name` stays generated.
        assert!(!overrides("CreateAlias").contains_key("Name"));
        assert_eq!(overrides("CreateAlias")["FunctionVersion"], "1");
        assert_eq!(overrides("PublishVersion")["CodeSha256"], "sha");
        assert_eq!(overrides("RemovePermission")["StatementId"], STATEMENT);
        assert!(!overrides("AddPermission").contains_key("StatementId"));
        let layer = overrides("GetLayerVersion");
        assert_eq!(layer["LayerName"], "l");
        assert_eq!(layer["VersionNumber"], 3);
        assert_eq!(
            overrides("GetLayerVersionByArn")["Arn"],
            "arn:aws:lambda:us-east-1:123456789012:layer:l:3"
        );
        assert_eq!(overrides("GetEventSourceMapping")["UUID"], "uuid");
        assert_eq!(
            overrides("CapacityProviderName_missing_op").get("CapacityProviderName"),
            None
        );
        assert_eq!(
            overrides("GetCapacityProvider")["CapacityProviderName"],
            "cp"
        );
        // CreateFunction keeps its generated name, attaches the seeded layer
        // and code signing config.
        let create = overrides("CreateFunction");
        assert!(!create.contains_key("FunctionName"));
        assert_eq!(
            create["Layers"][0],
            "arn:aws:lambda:us-east-1:123456789012:layer:l:3"
        );
        assert_eq!(create["CodeSigningConfigArn"], "csc-arn");
    }

    #[test]
    fn removers_reseed_every_variant() {
        assert!(consumes_seed("DeleteAlias"));
        assert!(consumes_seed("RemoveLayerVersionPermission"));
        assert!(!consumes_seed("GetAlias"));
        assert!(!consumes_seed("UpdateAlias"));
    }

    #[test]
    fn segments_are_percent_encoded() {
        assert_eq!(segment("arn:aws:x/y"), "arn%3Aaws%3Ax%2Fy");
    }
}
