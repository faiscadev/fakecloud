//! `LambdaService` `init` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    /// Determine the action from the HTTP method and path segments.
    /// Lambda uses REST-style routing:
    ///   POST   /2015-03-31/functions                         -> CreateFunction
    ///   GET    /2015-03-31/functions                         -> ListFunctions
    ///   GET    /2015-03-31/functions/{name}                  -> GetFunction
    ///   DELETE /2015-03-31/functions/{name}                  -> DeleteFunction
    ///   POST   /2015-03-31/functions/{name}/invocations      -> Invoke
    ///   POST   /2015-03-31/functions/{name}/versions         -> PublishVersion
    ///   POST   /2015-03-31/event-source-mappings             -> CreateEventSourceMapping
    ///   GET    /2015-03-31/event-source-mappings             -> ListEventSourceMappings
    ///   GET    /2015-03-31/event-source-mappings/{uuid}      -> GetEventSourceMapping
    ///   DELETE /2015-03-31/event-source-mappings/{uuid}      -> DeleteEventSourceMapping
    pub(crate) fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Option<String>)> {
        let segs = &req.path_segments;
        if segs.is_empty() {
            return None;
        }
        // The Lambda data API uses many date prefixes (one per
        // operation family). Recognise any well-formed YYYY-MM-DD
        // prefix and route based on the path structure that follows.
        let prefix = segs[0].as_str();

        // Account settings + InvokeAsync — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("account-settings") && req.method == Method::GET
        {
            return Some(("GetAccountSettings", None));
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("invoke-async")
            && req.method == Method::POST
        {
            return Some(("InvokeAsync", segs.get(2).map(|s| s.to_string())));
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("response-streaming-invocations")
            && req.method == Method::POST
        {
            return Some((
                "InvokeWithResponseStream",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Concurrency (reserved + provisioned) — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("concurrency")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionConcurrency", res)),
                Method::GET => Some(("GetFunctionConcurrency", res)),
                Method::DELETE => Some(("DeleteFunctionConcurrency", res)),
                _ => None,
            };
        }

        // Provisioned concurrency at any prefix. AWS overloads
        // `GET /functions/{name}/provisioned-concurrency` on the
        // `List=ALL` query parameter — with it, the op is
        // `ListProvisionedConcurrencyConfigs`; without, it's
        // `GetProvisionedConcurrencyConfig` (which needs a
        // `Qualifier`). Disambiguate before falling into the
        // per-method match.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("provisioned-concurrency")
        {
            let res = segs.get(2).map(|s| s.to_string());
            // AWS keys this overload on `List=ALL` specifically — any
            // other value (or a bare key) is a malformed request, not
            // an alias for the list op.
            if req.method == Method::GET
                && req.query_params.get("List").map(|v| v.as_str()) == Some("ALL")
            {
                return Some(("ListProvisionedConcurrencyConfigs", res));
            }
            return match req.method {
                Method::PUT => Some(("PutProvisionedConcurrencyConfig", res)),
                Method::GET => Some(("GetProvisionedConcurrencyConfig", res)),
                Method::DELETE => Some(("DeleteProvisionedConcurrencyConfig", res)),
                _ => None,
            };
        }
        // Legacy alias path (`provisioned-concurrency-configs`) — kept
        // for backward compatibility with conformance fixtures that
        // hand-craft the URL.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("provisioned-concurrency-configs")
            && req.method == Method::GET
        {
            return Some((
                "ListProvisionedConcurrencyConfigs",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Event invoke config — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("event-invoke-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::POST => Some(("PutFunctionEventInvokeConfig", res)),
                Method::PUT => Some(("UpdateFunctionEventInvokeConfig", res)),
                Method::GET => Some(("GetFunctionEventInvokeConfig", res)),
                Method::DELETE => Some(("DeleteFunctionEventInvokeConfig", res)),
                _ => None,
            };
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && (segs.get(3).map(|s| s.as_str()) == Some("event-invoke-config-list")
                || (segs.get(3).map(|s| s.as_str()) == Some("event-invoke-config")
                    && segs.get(4).map(|s| s.as_str()) == Some("list")))
            && req.method == Method::GET
        {
            return Some((
                "ListFunctionEventInvokeConfigs",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Recursion config — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("recursion-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionRecursionConfig", res)),
                Method::GET => Some(("GetFunctionRecursionConfig", res)),
                _ => None,
            };
        }

        // Runtime management config — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("runtime-management-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutRuntimeManagementConfig", res)),
                Method::GET => Some(("GetRuntimeManagementConfig", res)),
                _ => None,
            };
        }

        // Code signing config (function and global) — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("code-signing-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionCodeSigningConfig", res)),
                Method::GET => Some(("GetFunctionCodeSigningConfig", res)),
                Method::DELETE => Some(("DeleteFunctionCodeSigningConfig", res)),
                _ => None,
            };
        }
        if segs.get(1).map(|s| s.as_str()) == Some("code-signing-configs") {
            let res = segs.get(2).map(|s| s.to_string());
            return match (
                req.method.clone(),
                segs.len(),
                segs.get(3).map(|s| s.as_str()),
            ) {
                (Method::POST, 2, _) => Some(("CreateCodeSigningConfig", None)),
                (Method::GET, 2, _) => Some(("ListCodeSigningConfigs", None)),
                (Method::GET, 3, _) => Some(("GetCodeSigningConfig", res)),
                (Method::PUT, 3, _) => Some(("UpdateCodeSigningConfig", res)),
                (Method::DELETE, 3, _) => Some(("DeleteCodeSigningConfig", res)),
                (Method::GET, 4, Some("functions")) => {
                    Some(("ListFunctionsByCodeSigningConfig", res))
                }
                _ => None,
            };
        }

        // Tags resource ARN at any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("tags") && segs.len() >= 3 {
            let res = segs[2..].join("/");
            return match req.method {
                Method::POST => Some(("TagResource", Some(res))),
                Method::DELETE => Some(("UntagResource", Some(res))),
                Method::GET => Some(("ListTags", Some(res))),
                _ => None,
            };
        }

        // Function URL config + scaling config (any prefix).
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("url")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::POST => Some(("CreateFunctionUrlConfig", res)),
                Method::GET => Some(("GetFunctionUrlConfig", res)),
                Method::PUT => Some(("UpdateFunctionUrlConfig", res)),
                Method::DELETE => Some(("DeleteFunctionUrlConfig", res)),
                _ => None,
            };
        }
        if segs.get(1).map(|s| s.as_str()) == Some("function-urls")
            && segs.len() == 2
            && req.method == Method::GET
        {
            return Some(("ListFunctionUrlConfigs", None));
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("urls")
            && req.method == Method::GET
        {
            return Some(("ListFunctionUrlConfigs", segs.get(2).map(|s| s.to_string())));
        }
        // Function scaling config — AWS uses
        // `/2025-11-30/functions/{name}/function-scaling-config` (and the
        // earlier `scaling-config` path on event-source-mappings was
        // a different operation that no longer exists in the SDK).
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("function-scaling-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionScalingConfig", res)),
                Method::GET => Some(("GetFunctionScalingConfig", res)),
                _ => None,
            };
        }

        // NOTE: concurrency, event-invoke-config, recursion-config, and
        // code-signing-configs routes are all handled by the prefix-agnostic
        // blocks above. The previously-present date-specific blocks were
        // dead code.

        // /2018-10-31/layers
        if prefix == "2018-10-31" && segs.get(1).map(|s| s.as_str()) == Some("layers") {
            let layer = segs.get(2).map(|s| s.to_string());
            let third = segs.get(3).map(|s| s.as_str());
            let version = segs.get(4).map(|s| s.to_string());
            return match (&req.method, segs.len(), third, version.is_some()) {
                (&Method::GET, 2, _, _) => Some(("ListLayers", None)),
                (&Method::POST, 4, Some("versions"), false) => Some(("PublishLayerVersion", layer)),
                (&Method::GET, 4, Some("versions"), false) => {
                    // `GET .../versions` is `ListLayerVersions`; the same
                    // path with a trailing slash is a malformed
                    // `GetLayerVersion` call whose `VersionNumber` httpLabel
                    // was elided. Route there so the handler can reject.
                    if req.raw_path.ends_with("/versions/") {
                        Some(("GetLayerVersion", layer))
                    } else {
                        Some(("ListLayerVersions", layer))
                    }
                }
                (&Method::GET, 5, Some("versions"), true) => Some(("GetLayerVersion", version)),
                (&Method::DELETE, 5, Some("versions"), true) => {
                    Some(("DeleteLayerVersion", version))
                }
                // `DELETE .../versions/` (trailing slash, no VersionNumber
                // segment) is a malformed `DeleteLayerVersion` call — route
                // there so the handler can reject the missing path label.
                (&Method::DELETE, 4, Some("versions"), false)
                    if req.raw_path.ends_with("/versions/") =>
                {
                    Some(("DeleteLayerVersion", layer))
                }
                (&Method::GET, 6, Some("versions"), true)
                    if segs.get(5).map(|s| s.as_str()) == Some("policy") =>
                {
                    Some(("GetLayerVersionPolicy", version))
                }
                (&Method::POST, 6, Some("versions"), true)
                    if segs.get(5).map(|s| s.as_str()) == Some("policy") =>
                {
                    Some(("AddLayerVersionPermission", version))
                }
                (&Method::DELETE, 7, Some("versions"), true)
                    if segs.get(5).map(|s| s.as_str()) == Some("policy") =>
                {
                    Some(("RemoveLayerVersionPermission", version))
                }
                _ => None,
            };
        }

        // /2018-10-31/layers-by-arn
        if prefix == "2018-10-31"
            && segs.get(1).map(|s| s.as_str()) == Some("layers-by-arn")
            && req.method == Method::GET
        {
            return Some(("GetLayerVersionByArn", None));
        }

        // NOTE: 2021-10-31/functions/{name}/url and ListFunctionUrlConfigs
        // are handled by the prefix-agnostic blocks above.

        // /2025-11-30/capacity-providers — Lambda Workflows.
        if prefix == "2025-11-30" && segs.get(1).map(|s| s.as_str()) == Some("capacity-providers") {
            let name = segs.get(2).map(|s| s.to_string());
            // Empty CapacityProviderName label (negative-omit probe) reaches
            // here with one fewer segment than the model URI implies. Route
            // each method to its real op with an empty name so the validation
            // layer surfaces a declared InvalidParameterValueException
            // rather than degrading silently into List or returning 501.
            if segs.len() == 2 && req.raw_path.ends_with("/capacity-providers/") {
                return match req.method {
                    Method::GET => Some(("GetCapacityProvider", Some(String::new()))),
                    Method::PUT => Some(("UpdateCapacityProvider", Some(String::new()))),
                    Method::DELETE => Some(("DeleteCapacityProvider", Some(String::new()))),
                    _ => None,
                };
            }
            // Empty-name recovery: only when the wire path literally has
            // `//function-versions` (collapsed middle segment), not when
            // a provider is genuinely named "function-versions" — see
            // Cubic on PR #1474.
            if req
                .raw_path
                .contains("/capacity-providers//function-versions")
                && req.method == Method::GET
            {
                return Some((
                    "ListFunctionVersionsByCapacityProvider",
                    Some(String::new()),
                ));
            }
            return match (
                req.method.clone(),
                segs.len(),
                segs.get(3).map(|s| s.as_str()),
            ) {
                (Method::POST, 2, _) => Some(("CreateCapacityProvider", None)),
                (Method::GET, 2, _) => Some(("ListCapacityProviders", None)),
                (Method::GET, 3, _) => Some(("GetCapacityProvider", name)),
                (Method::PUT, 3, _) => Some(("UpdateCapacityProvider", name)),
                (Method::DELETE, 3, _) => Some(("DeleteCapacityProvider", name)),
                (Method::GET, 4, Some("function-versions")) => {
                    Some(("ListFunctionVersionsByCapacityProvider", name))
                }
                _ => None,
            };
        }

        // /2025-12-01/durable-executions and durable-execution-callbacks.
        if prefix == "2025-12-01" {
            let collection = segs.get(1).map(|s| s.as_str());
            let arn = segs.get(2).map(|s| s.to_string());
            let action = segs.get(3).map(|s| s.as_str());
            // Empty path label recovery: a negative-omit / negative-too-short
            // probe substituted an empty string into the URI template,
            // leaving a literal `//` in the wire path. The dispatcher's
            // segment splitter filters those out, hiding the bug behind a
            // shortened `segs` vector and a 501 ActionNotImplemented. We
            // catch the literal `//` form so the request reaches the right
            // handler with an empty identifier and the validation layer
            // surfaces a declared 400 InvalidParameterValueException —
            // without shadowing legitimate identifiers like "history" or
            // "state" (an ARN named "history" is rare but allowed).
            if req.raw_path.contains("/durable-executions//") {
                if req.raw_path.ends_with("/checkpoint") && req.method == Method::POST {
                    return Some(("CheckpointDurableExecution", Some(String::new())));
                }
                if req.raw_path.ends_with("/stop") && req.method == Method::POST {
                    return Some(("StopDurableExecution", Some(String::new())));
                }
                if req.raw_path.ends_with("/history") && req.method == Method::GET {
                    return Some(("GetDurableExecutionHistory", Some(String::new())));
                }
                if req.raw_path.ends_with("/state") && req.method == Method::GET {
                    return Some(("GetDurableExecutionState", Some(String::new())));
                }
            }
            if req.raw_path.ends_with("/durable-executions/") && req.method == Method::GET {
                return Some(("GetDurableExecution", Some(String::new())));
            }
            if req.raw_path.contains("/durable-execution-callbacks//") {
                if req.raw_path.ends_with("/succeed") && req.method == Method::POST {
                    return Some(("SendDurableExecutionCallbackSuccess", Some(String::new())));
                }
                if req.raw_path.ends_with("/fail") && req.method == Method::POST {
                    return Some(("SendDurableExecutionCallbackFailure", Some(String::new())));
                }
                if req.raw_path.ends_with("/heartbeat") && req.method == Method::POST {
                    return Some(("SendDurableExecutionCallbackHeartbeat", Some(String::new())));
                }
            }
            if req.raw_path.contains("/functions//durable-executions") && req.method == Method::GET
            {
                return Some(("ListDurableExecutionsByFunction", Some(String::new())));
            }
            match (req.method.clone(), collection, segs.len(), action) {
                (Method::GET, Some("durable-executions"), 3, _) => {
                    return Some(("GetDurableExecution", arn));
                }
                (Method::GET, Some("durable-executions"), 4, Some("history")) => {
                    return Some(("GetDurableExecutionHistory", arn));
                }
                (Method::GET, Some("durable-executions"), 4, Some("state")) => {
                    return Some(("GetDurableExecutionState", arn));
                }
                (Method::POST, Some("durable-executions"), 4, Some("checkpoint")) => {
                    return Some(("CheckpointDurableExecution", arn));
                }
                (Method::POST, Some("durable-executions"), 4, Some("stop")) => {
                    return Some(("StopDurableExecution", arn));
                }
                (Method::POST, Some("durable-execution-callbacks"), 4, Some("succeed")) => {
                    return Some(("SendDurableExecutionCallbackSuccess", arn));
                }
                (Method::POST, Some("durable-execution-callbacks"), 4, Some("fail")) => {
                    return Some(("SendDurableExecutionCallbackFailure", arn));
                }
                (Method::POST, Some("durable-execution-callbacks"), 4, Some("heartbeat")) => {
                    return Some(("SendDurableExecutionCallbackHeartbeat", arn));
                }
                (Method::GET, Some("functions"), 4, Some("durable-executions")) => {
                    return Some(("ListDurableExecutionsByFunction", arn));
                }
                _ => {}
            }
        }

        if prefix != "2015-03-31" {
            return None;
        }

        let collection = segs.get(1).map(|s| s.as_str());
        let resource = segs.get(2).map(|s| s.to_string());
        let third = segs.get(3).map(|s| s.as_str());
        let fourth = segs.get(4).map(|s| s.as_str());

        // NOTE: We intentionally do not try to disambiguate URLs that
        // collapsed because an httpLabel was elided (e.g.
        // `/functions/` vs `/functions`). Real AWS treats both as
        // `ListFunctions`, and botocore actually *serialized*
        // `ListFunctions` as `GET /2015-03-31/functions/` (trailing
        // slash) until botocore 1.40 (mid-2025) — so the entire pre-1.40
        // install base (pinned boto3, older CLI images) sends that form
        // (issue #1645). Routing it to `GetFunction` broke
        // `aws lambda list-functions` for those clients with a confusing
        // "FunctionName is required". Synthetic conformance probes that
        // elide a required path identifier are inherently un-faithful to
        // the SDK; the elided-FunctionName probe is steered to a sentinel
        // path in the conformance harness instead of relying on this
        // route, so accept both `/functions` forms as `ListFunctions`.

        let action = match (&req.method, segs.len(), collection, third) {
            (&Method::POST, 2, Some("functions"), _) => "CreateFunction",
            // Both `GET .../functions` and `GET .../functions/` are
            // `ListFunctions` — real AWS tolerates the trailing slash and
            // pre-1.40 botocore always sent it.
            (&Method::GET, 2, Some("functions"), _) => "ListFunctions",
            (&Method::GET, 3, Some("functions"), _) => "GetFunction",
            (&Method::DELETE, 3, Some("functions"), _) => "DeleteFunction",
            (&Method::POST, 4, Some("functions"), Some("invocations")) => "Invoke",
            (&Method::POST, 4, Some("functions"), Some("invoke-async")) => "InvokeAsync",
            (&Method::POST, 4, Some("functions"), Some("response-streaming-invocations")) => {
                "InvokeWithResponseStream"
            }
            (&Method::POST, 4, Some("functions"), Some("versions")) => "PublishVersion",
            (&Method::GET, 4, Some("functions"), Some("versions")) => "ListVersionsByFunction",
            (&Method::POST, 4, Some("functions"), Some("policy")) => "AddPermission",
            (&Method::GET, 4, Some("functions"), Some("policy")) => "GetPolicy",
            (&Method::DELETE, 5, Some("functions"), Some("policy")) => "RemovePermission",
            (&Method::POST, 4, Some("functions"), Some("aliases")) => "CreateAlias",
            (&Method::GET, 4, Some("functions"), Some("aliases")) => {
                // `GET .../aliases` (no trailing slash) is `ListAliases`.
                // `GET .../aliases/` (trailing slash) is `GetAlias` with an
                // empty `Name` — route there so it can reject the missing
                // path label rather than silently degrading to a list.
                if req.raw_path.ends_with("/aliases/") {
                    "GetAlias"
                } else {
                    "ListAliases"
                }
            }
            (&Method::GET, 5, Some("functions"), Some("aliases")) => "GetAlias",
            (&Method::PUT, 5, Some("functions"), Some("aliases")) => "UpdateAlias",
            (&Method::DELETE, 5, Some("functions"), Some("aliases")) => "DeleteAlias",
            (&Method::GET, 4, Some("functions"), Some("configuration")) => {
                "GetFunctionConfiguration"
            }
            (&Method::PUT, 4, Some("functions"), Some("configuration")) => {
                "UpdateFunctionConfiguration"
            }
            (&Method::PUT, 4, Some("functions"), Some("code")) => "UpdateFunctionCode",
            (&Method::PUT, 4, Some("functions"), Some("concurrency")) => "PutFunctionConcurrency",
            (&Method::GET, 4, Some("functions"), Some("concurrency")) => "GetFunctionConcurrency",
            (&Method::DELETE, 4, Some("functions"), Some("concurrency")) => {
                "DeleteFunctionConcurrency"
            }
            (&Method::PUT, 4, Some("functions"), Some("provisioned-concurrency")) => {
                "PutProvisionedConcurrencyConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("provisioned-concurrency")) => {
                "GetProvisionedConcurrencyConfig"
            }
            (&Method::DELETE, 4, Some("functions"), Some("provisioned-concurrency")) => {
                "DeleteProvisionedConcurrencyConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("provisioned-concurrency-configs")) => {
                "ListProvisionedConcurrencyConfigs"
            }
            (&Method::PUT, 4, Some("functions"), Some("event-invoke-config")) => {
                "UpdateFunctionEventInvokeConfig"
            }
            (&Method::POST, 4, Some("functions"), Some("event-invoke-config")) => {
                "PutFunctionEventInvokeConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("event-invoke-config")) => {
                "GetFunctionEventInvokeConfig"
            }
            (&Method::DELETE, 4, Some("functions"), Some("event-invoke-config")) => {
                "DeleteFunctionEventInvokeConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("event-invoke-config-list")) => {
                "ListFunctionEventInvokeConfigs"
            }
            (&Method::PUT, 4, Some("functions"), Some("code-signing-config")) => {
                "PutFunctionCodeSigningConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("code-signing-config")) => {
                "GetFunctionCodeSigningConfig"
            }
            (&Method::DELETE, 4, Some("functions"), Some("code-signing-config")) => {
                "DeleteFunctionCodeSigningConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("runtime-management-config")) => {
                "PutRuntimeManagementConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("runtime-management-config")) => {
                "GetRuntimeManagementConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("function-scaling-config")) => {
                "PutFunctionScalingConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("function-scaling-config")) => {
                "GetFunctionScalingConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("recursion-config")) => {
                "PutFunctionRecursionConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("recursion-config")) => {
                "GetFunctionRecursionConfig"
            }
            (&Method::POST, 2, Some("event-source-mappings"), _) => "CreateEventSourceMapping",
            (&Method::GET, 2, Some("event-source-mappings"), _) => "ListEventSourceMappings",
            (&Method::GET, 3, Some("event-source-mappings"), _) => "GetEventSourceMapping",
            (&Method::PUT, 3, Some("event-source-mappings"), _) => "UpdateEventSourceMapping",
            (&Method::DELETE, 3, Some("event-source-mappings"), _) => "DeleteEventSourceMapping",
            (&Method::POST, 3, Some("tags"), _) => "TagResource",
            (&Method::DELETE, 3, Some("tags"), _) => "UntagResource",
            (&Method::GET, 3, Some("tags"), _) => "ListTags",
            _ => return None,
        };
        let _ = fourth;

        Some((action, resource))
    }
}
