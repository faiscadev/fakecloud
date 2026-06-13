use axum::body::Body;
use axum::extract::{ConnectInfo, Extension, Query};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::auth::{
    is_root_bypass, ConditionContext, CredentialResolver, IamMode, IamPolicyEvaluator, Principal,
    PrincipalType, ResourcePolicyProvider,
};
use crate::protocol::{self, AwsProtocol};
use crate::registry::ServiceRegistry;
use crate::service::{AwsRequest, ResponseBody};

/// The main dispatch handler. All HTTP requests come through here.
pub async fn dispatch(
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    Extension(registry): Extension<Arc<ServiceRegistry>>,
    Extension(config): Extension<Arc<DispatchConfig>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Response<Body> {
    let remote_addr = Some(remote_addr);
    let request_id = uuid::Uuid::new_v4().to_string();

    let (parts, body) = request.into_parts();

    // Streaming opt-in: if the route is a known large-body S3 / ECR
    // upload, we skip the buffered `to_bytes` step entirely and hand
    // the raw body to the service handler. The handler spills it to
    // disk on the fly. Header-only detection covers every streaming
    // candidate (none of them rely on form-body sniffing).
    let stream_route = streaming_route(
        &parts.method,
        parts.uri.path(),
        &parts.headers,
        &query_params,
    );
    let header_only = protocol::detect_service_headers_only(&parts.headers, &query_params);
    let stream_dispatch = match (&stream_route, &header_only) {
        // Header-only detection agrees with the URL match — covers S3
        // PUT object (SigV4 service=s3 in Authorization).
        (Some(sr), Some(detected)) if sr.0 == detected.service => Some(detected.clone()),
        // ECR OCI v2 blob upload has no AWS auth header; the path
        // alone (`/v2/.../blobs/uploads/...`) tells us the route is
        // ECR. Synthesize a DetectedRequest so dispatch picks the
        // streaming path. Same special-case the buffered branch
        // applies on detect_service None (see below).
        (Some((service, _)), None) if *service == "ecr" => Some(protocol::DetectedRequest {
            service: "ecr".to_string(),
            action: String::new(),
            protocol: AwsProtocol::Rest,
        }),
        _ => None,
    };

    let (body_bytes, body_stream) = if stream_dispatch.is_some() {
        (Bytes::new(), Some(body))
    } else {
        // Buffered path: materialize the body into memory under the
        // configured cap. `FAKECLOUD_MAX_REQUEST_BODY_BYTES` (default
        // 1 GiB) caps non-streaming requests; streaming routes have no
        // cap because nothing materializes the entire body in RAM.
        let max_body_bytes = max_request_body_bytes();
        match axum::body::to_bytes(body, max_body_bytes).await {
            Ok(b) => (b, None),
            Err(_) => {
                return build_error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "RequestEntityTooLarge",
                    "Request body too large",
                    &request_id,
                    AwsProtocol::Query,
                );
            }
        }
    };

    // Detect service and action
    let detected = if let Some(d) = stream_dispatch {
        d
    } else {
        match protocol::detect_service(&parts.headers, &query_params, &body_bytes) {
            Some(d) => d,
            None => {
                // OPTIONS requests (CORS preflight) don't carry Authorization headers.
                // Route them to S3 since S3 is the only REST service that handles CORS.
                // Note: API Gateway CORS preflight is not fully supported in this emulator
                // because we can't distinguish between S3 and API Gateway OPTIONS requests
                // without additional context (in real AWS, they have different domains).
                if parts.method == http::Method::OPTIONS {
                    protocol::DetectedRequest {
                        service: "s3".to_string(),
                        action: String::new(),
                        protocol: AwsProtocol::Rest,
                    }
                } else if parts.uri.path() == "/v2" || parts.uri.path().starts_with("/v2/") {
                    // OCI Distribution v2 protocol. Docker CLI / OCI clients
                    // use Basic auth (not SigV4) and GET /v2/ with no body,
                    // so this must be matched before the apigateway fallback.
                    protocol::DetectedRequest {
                        service: "ecr".to_string(),
                        action: String::new(),
                        protocol: AwsProtocol::Rest,
                    }
                } else if !parts.uri.path().starts_with("/_") {
                    // Requests without AWS auth that don't match any service might be
                    // API Gateway execute API calls (plain HTTP without signatures).
                    // Route them to apigateway service which will validate if a matching
                    // API/stage exists. Skip special FakeCloud endpoints (/_*).
                    protocol::DetectedRequest {
                        service: "apigateway".to_string(),
                        action: String::new(),
                        protocol: AwsProtocol::RestJson,
                    }
                } else {
                    return build_error_response(
                        StatusCode::BAD_REQUEST,
                        "MissingAction",
                        "Could not determine target service or action from request",
                        &request_id,
                        AwsProtocol::Query,
                    );
                }
            }
        }
    };

    // Bedrock-agent and bedrock-runtime both send `bedrock` in the SigV4
    // credential scope, but bedrock-agent has its own service handler.
    // Disambiguate based on the request path.
    let detected = if detected.service == "bedrock" {
        let first_seg = parts.uri.path().split('/').nth(1);
        if matches!(
            first_seg,
            Some(
                "agents"
                    | "knowledgebases"
                    | "flows"
                    | "prompts"
                    | "tags"
                    | "retrieveAndGenerate"
                    | "retrieveAndGenerateStream"
                    | "optimize-prompt"
                    | "sessions"
                    | "invocations"
                    | "generate-query"
                    | "rerank"
            )
        ) {
            // Further disambiguate runtime vs control plane for agents/flows paths
            let segs: Vec<&str> = parts.uri.path().split('/').collect();
            let is_runtime = matches!(
                segs.as_slice(),
                ["", "agents", _, "agentAliases", _, ..]  // InvokeAgent
                    | ["", "flows", _, "aliases", _]   // InvokeFlow
                    | ["", "knowledgebases", _, "retrieve"] // Retrieve
                    | ["", "retrieveAndGenerate"]
                    | ["", "retrieveAndGenerateStream"]
                    | ["", "optimize-prompt"]
                    | ["", "sessions", ..]
                    | ["", "invocations", ..]
                    | ["", "generate-query"]
                    | ["", "rerank"]
            );
            if is_runtime {
                protocol::DetectedRequest {
                    service: "bedrock-agent-runtime".to_string(),
                    ..detected
                }
            } else {
                protocol::DetectedRequest {
                    service: "bedrock-agent".to_string(),
                    ..detected
                }
            }
        } else {
            detected
        }
    } else {
        detected
    };

    // Look up service
    let service = match registry.get(&detected.service) {
        Some(s) => s,
        None => {
            return build_error_response(
                detected.protocol.error_status(),
                "UnknownService",
                &format!("Service '{}' is not available", detected.service),
                &request_id,
                detected.protocol,
            );
        }
    };

    // Extract region and access key from auth header (or presigned query).
    let auth_header = parts
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let header_info = fakecloud_aws::sigv4::parse_sigv4(auth_header);
    let presigned_info = if header_info.is_none() {
        // Presigned URL: credentials live in the query string.
        fakecloud_aws::sigv4::parse_sigv4_presigned(&query_params).map(|p| p.as_info())
    } else {
        None
    };
    let sigv4_info = header_info.or(presigned_info);
    let access_key_id = sigv4_info.as_ref().map(|info| info.access_key.clone());

    // Host-header routing hint: LocalStack-shaped
    // `<svc>.<region>.localhost.localstack.cloud[:port]`, real-AWS
    // `<svc>.<region>.amazonaws.com`, and every S3 virtual-hosted variant
    // of both. Secondary region source and carries the bucket for
    // virtual-hosted S3 path rewrite.
    let host_info = protocol::parse_routing_host_from_headers(&parts.headers);

    let region = sigv4_info
        .map(|info| info.region)
        .or_else(|| host_info.as_ref().map(|h| h.region.clone()))
        .or_else(|| extract_region_from_user_agent(&parts.headers))
        .unwrap_or_else(|| config.region.clone());

    // Resolve the caller's principal up front so both SigV4 verification
    // (which needs the secret) and the service handler (which needs the
    // identity for GetCallerIdentity and IAM enforcement) share a single
    // lookup. The root-bypass AKID skips resolution entirely — `test`
    // credentials have no backing identity and must always pass.
    let caller_akid = access_key_id.as_deref().unwrap_or("");
    let resolved = if !caller_akid.is_empty() && !is_root_bypass(caller_akid) {
        config
            .credential_resolver
            .as_ref()
            .and_then(|r| r.resolve(caller_akid))
    } else {
        None
    };
    let caller_principal = resolved.as_ref().map(|r| r.principal.clone());
    let caller_session_policies = resolved
        .as_ref()
        .map(|r| r.session_policies.clone())
        .unwrap_or_default();

    // Opt-in SigV4 cryptographic verification. Runs before the service
    // handler so a failing signature never reaches business logic. The
    // reserved `test*` root identity short-circuits verification to keep
    // local-dev workflows frictionless.
    if config.verify_sigv4 && !is_root_bypass(caller_akid) && config.credential_resolver.is_some() {
        let amz_date = parts
            .headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok());
        let parsed = fakecloud_aws::sigv4::parse_sigv4_header(auth_header, amz_date)
            .or_else(|| fakecloud_aws::sigv4::parse_sigv4_presigned(&query_params));
        let parsed = match parsed {
            Some(p) => p,
            None => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "IncompleteSignature",
                    "Request is missing or has a malformed AWS Signature",
                    &request_id,
                    detected.protocol,
                );
            }
        };
        let resolved_for_verify = match resolved.as_ref() {
            Some(r) => r,
            None => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "InvalidClientTokenId",
                    "The security token included in the request is invalid",
                    &request_id,
                    detected.protocol,
                );
            }
        };
        let headers_vec = fakecloud_aws::sigv4::headers_from_http(&parts.headers);
        let raw_query_for_verify = parts.uri.query().unwrap_or("").to_string();
        let verify_req = fakecloud_aws::sigv4::VerifyRequest {
            method: parts.method.as_str(),
            path: parts.uri.path(),
            query: &raw_query_for_verify,
            headers: &headers_vec,
            body: &body_bytes,
        };
        match fakecloud_aws::sigv4::verify(
            &parsed,
            &verify_req,
            &resolved_for_verify.secret_access_key,
            chrono::Utc::now(),
        ) {
            Ok(()) => {}
            Err(fakecloud_aws::sigv4::SigV4Error::RequestTimeTooSkewed { .. }) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "RequestTimeTooSkewed",
                    "The difference between the request time and the current time is too large",
                    &request_id,
                    detected.protocol,
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::InvalidDate(msg)) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "IncompleteSignature",
                    &format!("Invalid x-amz-date: {msg}"),
                    &request_id,
                    detected.protocol,
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::Malformed(msg)) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "IncompleteSignature",
                    &format!("Malformed SigV4 signature: {msg}"),
                    &request_id,
                    detected.protocol,
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::SignatureMismatch) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "SignatureDoesNotMatch",
                    "The request signature we calculated does not match the signature you provided",
                    &request_id,
                    detected.protocol,
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::PresignedUrlExpired { .. }) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "Request has expired",
                    &request_id,
                    detected.protocol,
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::InvalidPresignExpires(_)) => {
                return build_error_response(
                    StatusCode::BAD_REQUEST,
                    "AuthorizationQueryParametersError",
                    "X-Amz-Expires must be a number between 1 and 604800 seconds",
                    &request_id,
                    detected.protocol,
                );
            }
        }
    }

    // Build path segments. For S3 virtual-hosted-style requests the bucket
    // lives in the Host header, not the path — prepend it so the S3 handler
    // sees a uniform path-style request. SigV4 verification above already
    // ran against the wire path, so this rewrite is signature-safe.
    let wire_path = parts.uri.path();
    let path = if detected.service == "s3" {
        if let Some(bucket) = host_info.as_ref().and_then(|h| h.bucket.as_deref()) {
            let prefix_with_slash = format!("/{bucket}/");
            let is_bucket_root = wire_path.trim_end_matches('/') == format!("/{bucket}");
            if wire_path.starts_with(&prefix_with_slash) || is_bucket_root {
                wire_path.to_string()
            } else if wire_path == "/" || wire_path.is_empty() {
                format!("/{bucket}")
            } else {
                format!("/{bucket}{wire_path}")
            }
        } else {
            wire_path.to_string()
        }
    } else {
        wire_path.to_string()
    };
    let raw_query = parts.uri.query().unwrap_or("").to_string();
    let path_segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // For JSON protocol, validate that non-empty bodies are valid JSON
    if detected.protocol == AwsProtocol::Json
        && !body_bytes.is_empty()
        && serde_json::from_slice::<serde_json::Value>(&body_bytes).is_err()
    {
        return build_error_response(
            StatusCode::BAD_REQUEST,
            "SerializationException",
            "Start of structure or map found where not expected",
            &request_id,
            AwsProtocol::Json,
        );
    }

    // Merge query params with form body params for Query protocol
    let mut all_params = query_params;
    if detected.protocol == AwsProtocol::Query {
        let body_params = protocol::parse_query_body(&body_bytes);
        for (k, v) in body_params {
            all_params.entry(k).or_insert(v);
        }
    }

    let aws_request = AwsRequest {
        service: detected.service.clone(),
        action: detected.action.clone(),
        region,
        account_id: caller_principal
            .as_ref()
            .map(|p| p.account_id.clone())
            .unwrap_or_else(|| config.account_id.clone()),
        request_id: request_id.clone(),
        headers: parts.headers,
        query_params: all_params,
        body: body_bytes,
        body_stream: parking_lot::Mutex::new(body_stream),
        path_segments,
        raw_path: path,
        raw_query,
        method: parts.method,
        is_query_protocol: detected.protocol == AwsProtocol::Query,
        access_key_id,
        principal: caller_principal,
    };

    tracing::info!(
        service = %aws_request.service,
        action = %aws_request.action,
        request_id = %aws_request.request_id,
        "handling request"
    );

    // Opt-in IAM identity-policy enforcement. Runs before the service
    // handler so a deny never reaches business logic. Root principals
    // (both `test*` bypass AKIDs and the account's IAM root) are exempt,
    // matching AWS behavior. Services that haven't opted in via
    // `iam_enforceable()` are transparently skipped — the startup log
    // lists which services are under enforcement so users always know.
    if config.iam_mode.is_enabled()
        && service.iam_enforceable()
        && !is_root_bypass(aws_request.access_key_id.as_deref().unwrap_or(""))
    {
        if let Some(evaluator) = config.policy_evaluator.as_ref() {
            if let Some(principal) = aws_request.principal.as_ref() {
                if !principal.is_root() {
                    if let Some(iam_action) = service.iam_action_for(&aws_request) {
                        let mut condition_context = build_condition_context(
                            principal,
                            remote_addr,
                            &aws_request.region,
                            is_secure_transport(&aws_request.headers),
                        );
                        // F3 keys riding on the resolved credential. STS
                        // populates these at mint time so subsequent
                        // requests under the credential can be evaluated
                        // against `aws:MultiFactorAuthPresent`,
                        // `aws:MultiFactorAuthAge`, `aws:TokenIssueTime`,
                        // and `aws:FederatedProvider`. IAM user access
                        // keys carry none of these, matching AWS.
                        if let Some(rc) = resolved.as_ref() {
                            condition_context.aws_mfa_present = Some(rc.mfa_present);
                            condition_context.aws_token_issue_time = rc.token_issued_at;
                            condition_context.aws_federated_provider =
                                rc.federated_provider.clone();
                            // `aws:MultiFactorAuthAge` is "seconds since
                            // MFA was asserted" — computed at evaluation
                            // time from the token issue moment so the
                            // value increases monotonically as the session
                            // ages. Only set when the session was actually
                            // minted with MFA; otherwise the key is
                            // absent, matching AWS.
                            if rc.mfa_present {
                                if let Some(issued) = rc.token_issued_at {
                                    let age = chrono::Utc::now()
                                        .signed_duration_since(issued)
                                        .num_seconds()
                                        .max(0);
                                    condition_context.aws_mfa_age_seconds = Some(age);
                                }
                            }
                        }
                        condition_context.service_keys =
                            service.iam_condition_keys_for(&aws_request, &iam_action);

                        // ABAC: populate tag-based condition keys.
                        // aws:ResourceTag/*
                        match service.resource_tags_for(&iam_action.resource) {
                            Some(tags) => condition_context.resource_tags = Some(tags),
                            None => tracing::debug!(
                                target: "fakecloud::iam::audit",
                                service = %detected.service,
                                resource = %iam_action.resource,
                                "service does not expose resource tags for ABAC; skipping aws:ResourceTag/* evaluation"
                            ),
                        }
                        // aws:RequestTag/* + aws:TagKeys
                        match service.request_tags_from(&aws_request, iam_action.action) {
                            Some(tags) => condition_context.request_tags = Some(tags),
                            None => tracing::debug!(
                                target: "fakecloud::iam::audit",
                                service = %detected.service,
                                action = %iam_action.action_string(),
                                "service does not expose request tags for ABAC; skipping aws:RequestTag/* / aws:TagKeys evaluation"
                            ),
                        }
                        // aws:PrincipalTag/*
                        condition_context.principal_tags = principal.tags.clone();

                        // Phase 2: fetch the resource-based policy (if
                        // any) attached to the target resource and
                        // pass it to the evaluator alongside the
                        // principal's identity policies. The resource's
                        // owning account is parsed from the ARN (#381
                        // multi-account alignment); S3 ARNs have an
                        // empty account field, so we fall back to the
                        // server's configured account ID in that case.
                        let resource_policy_json =
                            config.resource_policy_provider.as_ref().and_then(|p| {
                                p.resource_policy(&detected.service, &iam_action.resource)
                            });
                        // Derive the resource-owning account. Prefer a provider
                        // lookup (S3 ARNs carry no account, so the bucket's
                        // owner is resolved from state — without this, account
                        // A reaching account B's bucket would be mis-read as
                        // same-account and skip B's bucket-policy requirement,
                        // bug-audit 2026-05-28, 5.3), then fall back to the
                        // account embedded in the ARN (SQS/SNS/Lambda/…), then
                        // to the caller's account for wildcard / unscoped
                        // actions (ListQueues, GetCallerIdentity).
                        let resource_account_id = config
                            .resource_policy_provider
                            .as_ref()
                            .and_then(|p| {
                                p.resource_owner_account(&detected.service, &iam_action.resource)
                            })
                            .or_else(|| parse_account_from_arn(&iam_action.resource))
                            .unwrap_or_else(|| principal.account_id.clone());
                        // SCP ceiling: resolve the inherited SCP chain
                        // for this principal (management accounts and
                        // service-linked roles come back as `None`, in
                        // which case the evaluator treats the layer as
                        // absent). Audit breadcrumbs emitted by the
                        // resolver itself, not here.
                        let scps = config
                            .scp_resolver
                            .as_ref()
                            .and_then(|r| r.scps_for(principal));
                        let decision = evaluator.evaluate_with_resource_policy(
                            principal,
                            &iam_action,
                            &condition_context,
                            resource_policy_json.as_deref(),
                            &resource_account_id,
                            &caller_session_policies,
                            scps.as_deref(),
                        );
                        if !decision.is_allow() {
                            tracing::warn!(
                                target: "fakecloud::iam::audit",
                                service = %detected.service,
                                action = %iam_action.action_string(),
                                resource = %iam_action.resource,
                                principal = %principal.arn,
                                resource_policy_present = resource_policy_json.is_some(),
                                decision = ?decision,
                                mode = %config.iam_mode,
                                request_id = %request_id,
                                "IAM policy evaluation denied request"
                            );
                            if config.iam_mode.is_strict() {
                                // Real AWS includes an "Encoded
                                // authorization failure message" suffix
                                // on AccessDeniedException — an opaque
                                // base64+zlib JSON blob that the caller
                                // can pass to STS
                                // `DecodeAuthorizationMessage` to
                                // recover the structured deny reason
                                // (action, principal, matched
                                // statements, condition context). We
                                // produce the same blob inline so
                                // existing tooling that decodes deny
                                // reasons works against fakecloud.
                                let context_summary = serde_json::json!({
                                    "aws:PrincipalArn": principal.arn,
                                    "aws:PrincipalAccount": principal.account_id,
                                    "aws:RequestedRegion": condition_context
                                        .aws_requested_region
                                        .clone()
                                        .unwrap_or_default(),
                                    "aws:SecureTransport": condition_context
                                        .aws_secure_transport
                                        .unwrap_or(false),
                                    "aws:Action": iam_action.action_string(),
                                    "aws:Resource": iam_action.resource,
                                    "decision": format!("{:?}", decision),
                                });
                                let action_string = iam_action.action_string();
                                let encoded = crate::auth_message::encode_deny(
                                    matches!(decision, crate::auth::IamDecision::ExplicitDeny),
                                    Some(&action_string),
                                    Some(&principal.arn),
                                    Vec::new(),
                                    Some(context_summary),
                                );
                                return build_error_response(
                                    StatusCode::FORBIDDEN,
                                    "AccessDeniedException",
                                    &format!(
                                        "User: {} is not authorized to perform: {} on resource: {} Encoded authorization failure message: {}",
                                        principal.arn,
                                        iam_action.action_string(),
                                        iam_action.resource,
                                        encoded,
                                    ),
                                    &request_id,
                                    detected.protocol,
                                );
                            }
                            // Soft mode: audit log already emitted; fall
                            // through to the handler.
                        }
                    } else {
                        // Service opted in but didn't return an IamAction
                        // for this specific operation — programming bug,
                        // surface it loudly in soft/strict mode so it's
                        // visible during rollout.
                        tracing::warn!(
                            target: "fakecloud::iam::audit",
                            service = %detected.service,
                            action = %aws_request.action,
                            "service is iam_enforceable but has no IamAction mapping for this action; skipping evaluation"
                        );
                    }
                }
            } else if !aws_request
                .access_key_id
                .as_deref()
                .unwrap_or("")
                .is_empty()
            {
                // IAM enforcement is on but the presented (non-test,
                // non-root) access key id resolved to no principal — an
                // unknown credential. This branch previously didn't exist,
                // so such a request fell straight through to the handler
                // with no policy evaluation, silently bypassing enforcement
                // whenever SigV4 verification was off (a user could defeat
                // their own deny policies just by sending a bogus AKID).
                // Bug-hunt 2026-06-13, finding 5.1. Real AWS rejects an
                // unknown access key id with InvalidClientTokenId before any
                // authorization. Mirror that in strict mode; in soft mode
                // audit and fall through so observation-only stays
                // non-disruptive.
                tracing::warn!(
                    target: "fakecloud::iam::audit",
                    service = %detected.service,
                    akid = %aws_request.access_key_id.as_deref().unwrap_or(""),
                    mode = %config.iam_mode,
                    request_id = %request_id,
                    "IAM enforcement on but access key id resolved to no principal (unknown credential)"
                );
                if config.iam_mode.is_strict() {
                    return build_error_response(
                        StatusCode::FORBIDDEN,
                        "InvalidClientTokenId",
                        "The security token included in the request is invalid",
                        &request_id,
                        detected.protocol,
                    );
                }
            }
        }
    }

    match service.handle(aws_request).await {
        Ok(resp) => {
            let mut builder = Response::builder()
                .status(resp.status)
                .header("x-amzn-requestid", &request_id)
                .header("x-amz-request-id", &request_id);

            if !resp.content_type.is_empty() {
                builder = builder.header("content-type", &resp.content_type);
            }

            let has_content_length = resp
                .headers
                .iter()
                .any(|(k, _)| k.as_str().eq_ignore_ascii_case("content-length"));

            for (k, v) in &resp.headers {
                builder = builder.header(k, v);
            }

            match resp.body {
                ResponseBody::Bytes(b) => builder.body(Body::from(b)).unwrap(),
                ResponseBody::File { file, size } => {
                    let stream = tokio_util::io::ReaderStream::new(file);
                    let body = Body::from_stream(stream);
                    if !has_content_length {
                        builder = builder.header("content-length", size.to_string());
                    }
                    builder.body(body).unwrap()
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                service = %detected.service,
                action = %detected.action,
                error = %err,
                "request failed"
            );
            let error_headers = err.response_headers().to_vec();
            let mut resp = build_error_response_with_fields(
                err.status(),
                err.code(),
                &err.message(),
                &request_id,
                detected.protocol,
                err.extra_fields(),
            );
            for (k, v) in &error_headers {
                if let (Ok(name), Ok(val)) = (
                    k.parse::<http::header::HeaderName>(),
                    v.parse::<http::header::HeaderValue>(),
                ) {
                    resp.headers_mut().insert(name, val);
                }
            }
            resp
        }
    }
}

/// Configuration passed to the dispatch handler.
#[derive(Clone)]
pub struct DispatchConfig {
    pub region: String,
    pub account_id: String,
    /// Whether to cryptographically verify SigV4 signatures on incoming
    /// requests. Wired through from `--verify-sigv4` /
    /// `FAKECLOUD_VERIFY_SIGV4`. Off by default.
    pub verify_sigv4: bool,
    /// IAM policy evaluation mode. Wired through from `--iam` /
    /// `FAKECLOUD_IAM`. Defaults to [`IamMode::Off`]. Actual evaluation is
    /// added in a later batch; today this field is plumbed but never
    /// consulted.
    pub iam_mode: IamMode,
    /// Resolves access key IDs to their secrets and owning principals.
    /// Required when `verify_sigv4` or `iam_mode != Off`. When `None`, both
    /// features gracefully degrade to off-by-default behavior.
    pub credential_resolver: Option<Arc<dyn CredentialResolver>>,
    /// Evaluates IAM identity policies for a resolved principal + action.
    /// Required when `iam_mode != Off`. When `None`, enforcement silently
    /// degrades to off even if `iam_mode` is set.
    pub policy_evaluator: Option<Arc<dyn IamPolicyEvaluator>>,
    /// Resolves resource-based policies (S3 bucket policies in the
    /// initial rollout) to hand to the evaluator alongside the
    /// principal's identity policies. `None` means the server was
    /// started without any resource-policy-owning service registered;
    /// dispatch then behaves as if no resource policy is attached to
    /// any resource, identical to the Phase 1 behavior.
    pub resource_policy_provider: Option<Arc<dyn ResourcePolicyProvider>>,
    /// Resolves the ordered SCP chain that applies to a principal's
    /// account (root-OU first, account-direct last). `None` means no
    /// organizations resolver has been registered — SCPs never gate
    /// any request in that case. Off-by-default matches the Batch 4
    /// contract: zero behavior change until a user calls
    /// `CreateOrganization` and the resolver is wired.
    pub scp_resolver: Option<Arc<dyn crate::auth::ScpResolver>>,
}

impl std::fmt::Debug for DispatchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchConfig")
            .field("region", &self.region)
            .field("account_id", &self.account_id)
            .field("verify_sigv4", &self.verify_sigv4)
            .field("iam_mode", &self.iam_mode)
            .field(
                "credential_resolver",
                &self
                    .credential_resolver
                    .as_ref()
                    .map(|_| "<CredentialResolver>"),
            )
            .field(
                "policy_evaluator",
                &self
                    .policy_evaluator
                    .as_ref()
                    .map(|_| "<IamPolicyEvaluator>"),
            )
            .field(
                "resource_policy_provider",
                &self
                    .resource_policy_provider
                    .as_ref()
                    .map(|_| "<ResourcePolicyProvider>"),
            )
            .field(
                "scp_resolver",
                &self.scp_resolver.as_ref().map(|_| "<ScpResolver>"),
            )
            .finish()
    }
}

impl DispatchConfig {
    /// Minimal constructor for tests and call sites that don't care about the
    /// opt-in security features.
    pub fn new(region: impl Into<String>, account_id: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            account_id: account_id.into(),
            verify_sigv4: false,
            iam_mode: IamMode::Off,
            credential_resolver: None,
            policy_evaluator: None,
            resource_policy_provider: None,
            scp_resolver: None,
        }
    }
}

/// Extract the 12-digit account ID segment from an AWS ARN.
///
/// ARNs follow `arn:<partition>:<service>:<region>:<account>:<resource>`.
/// Identifies routes that opt into streaming request bodies. Returns
/// `Some((service, action_hint))` when the dispatch path should hand
/// the raw body to the service handler unbuffered, otherwise `None`
/// for the default buffered path. The handler reads the stream via
/// [`crate::service::AwsRequest::take_body_stream`].
///
/// Streaming-eligible routes today:
///
/// * `s3` PUT object — `PUT /<bucket>/<key>` with a SigV4 (or
///   presigned) auth header. Covers PutObject, UploadPart, and
///   UploadPartCopy. The S3 service spills to disk via
///   [`fakecloud_persistence::BodySource::File`] when the stream is
///   present.
/// * `ecr` OCI Distribution v2 blob upload — `PATCH` and `PUT` on
///   `/v2/{name}/blobs/uploads/{uuid}`. The ECR service spools the
///   stream into a per-upload temp file before computing the digest.
fn streaming_route(
    method: &http::Method,
    path: &str,
    headers: &http::HeaderMap,
    query_params: &HashMap<String, String>,
) -> Option<(&'static str, &'static str)> {
    // ECR OCI v2 blob upload (PATCH chunk + final PUT).
    if (method == http::Method::PATCH || method == http::Method::PUT)
        && path.starts_with("/v2/")
        && path.contains("/blobs/uploads/")
    {
        return Some(("ecr", ""));
    }

    // S3 PutObject / UploadPart / UploadPartCopy. Detect either via
    // SigV4 service field in the Authorization header OR via a SigV4
    // presigned URL (X-Amz-Credential .../s3/...) OR a SigV2 presigned
    // URL (AWSAccessKeyId + Signature + Expires query parameters).
    if method == http::Method::PUT {
        let after = path.trim_start_matches('/');
        // Path-style PutObject is `PUT /<bucket>/<key>` (path contains a
        // slash); virtual-hosted-style is `PUT /<key>` with the bucket
        // in the Host header. For virtual-hosted, accept any non-empty
        // path so the key flows through the streaming dispatch — the
        // Host parser already routed this request to S3.
        let virtual_hosted_s3 = protocol::parse_routing_host_from_headers(headers)
            .filter(|h| h.service == "s3" && h.bucket.is_some())
            .is_some();
        if after.is_empty() || (!virtual_hosted_s3 && !after.contains('/')) {
            return None;
        }
        let header_s3 = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(fakecloud_aws::sigv4::parse_sigv4)
            .map(|info| info.service == "s3")
            .unwrap_or(false);
        let presigned_v4_s3 = query_params
            .get("X-Amz-Credential")
            .and_then(|c| c.split('/').nth(3).map(|s| s.to_string()))
            .map(|service| service == "s3")
            .unwrap_or(false);
        let presigned_v2 = query_params.contains_key("AWSAccessKeyId")
            && query_params.contains_key("Signature")
            && query_params.contains_key("Expires");
        if header_s3 || presigned_v4_s3 || presigned_v2 {
            return Some(("s3", ""));
        }
    }

    None
}

/// Default request-body buffering cap. fakecloud reads the entire
/// request body into memory before handing it to a service handler,
/// so this ceiling caps RAM usage per in-flight request.
///
/// Default 1 GiB — comfortably above legitimate single S3 PutObject
/// payloads (AWS recommends multipart above ~100 MiB) and each
/// multipart part dispatches through here separately. Override with
/// `FAKECLOUD_MAX_REQUEST_BODY_BYTES` (decimal bytes) when running
/// stress tests that push past the default.
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024 * 1024;

fn max_request_body_bytes() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FAKECLOUD_MAX_REQUEST_BODY_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_REQUEST_BODY_BYTES)
    })
}

/// For the cross-account decision in IAM enforcement, the "resource
/// account" is the ARN's account segment. Some services (notably S3)
/// produce ARNs with an empty account field — for those we return
/// `None` and let the caller fall back to the server's configured
/// account ID. Malformed or non-ARN strings also return `None`.
fn parse_account_from_arn(arn: &str) -> Option<String> {
    let mut parts = arn.splitn(6, ':');
    if parts.next()? != "arn" {
        return None;
    }
    let _partition = parts.next()?;
    let _service = parts.next()?;
    let _region = parts.next()?;
    let account = parts.next()?;
    // Resource segment must exist (parts.next().is_some()) for the ARN
    // to be well-formed, but we don't consume its value here.
    parts.next()?;
    if account.is_empty() {
        None
    } else {
        Some(account.to_string())
    }
}

/// Extract region from User-Agent header suffix `region/<region>`.
fn extract_region_from_user_agent(headers: &http::HeaderMap) -> Option<String> {
    let ua = headers.get("user-agent")?.to_str().ok()?;
    for part in ua.split_whitespace() {
        if let Some(region) = part.strip_prefix("region/") {
            if !region.is_empty() {
                return Some(region.to_string());
            }
        }
    }
    None
}

fn build_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    protocol: AwsProtocol,
) -> Response<Body> {
    build_error_response_with_fields(status, code, message, request_id, protocol, &[])
}

fn build_error_response_with_fields(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    protocol: AwsProtocol,
    extra_fields: &[(String, String)],
) -> Response<Body> {
    let (status, content_type, body) = match protocol {
        AwsProtocol::Query => {
            fakecloud_aws::error::xml_error_response(status, code, message, request_id)
        }
        AwsProtocol::Rest => fakecloud_aws::error::s3_xml_error_response_with_fields(
            status,
            code,
            message,
            request_id,
            extra_fields,
        ),
        AwsProtocol::Json | AwsProtocol::RestJson => {
            fakecloud_aws::error::json_error_response(status, code, message)
        }
    };

    // S3 (and other REST-XML services) place the error code in
    // `x-amz-error-code` so HEAD responses — which HTTP forbids from
    // carrying a body — still surface the code. AWS SDKs read this header
    // when the body is empty. Emit it on every error response so HEAD,
    // OPTIONS, and any client that strips the body still see the code.
    // Backend errors regularly include newlines (multi-line stderr from
    // docker/podman/etc.); HTTP header values reject control characters,
    // so sanitize before insertion or the builder rejects the response
    // and the connection drops.
    let safe_code = sanitize_header_value(code);
    let safe_message = sanitize_header_value(message);
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("x-amzn-requestid", request_id)
        .header("x-amz-request-id", request_id);
    if let Ok(v) = http::HeaderValue::from_str(&safe_code) {
        builder = builder.header("x-amz-error-code", v);
    }
    if let Ok(v) = http::HeaderValue::from_str(&safe_message) {
        builder = builder.header("x-amz-error-message", v);
    }
    builder.body(Body::from(body)).unwrap_or_else(|_| {
        // Builder only fails if a header is invalid; we sanitized the two
        // we control, so the remaining ones (content-type, request id) are
        // ASCII and safe. This fallback exists purely so we never panic.
        Response::new(Body::empty())
    })
}

/// Strip characters that HTTP header values reject (control bytes, CR/LF/TAB)
/// and truncate to a length that AWS SDKs handle cleanly. Backend tools
/// (docker, podman, kubectl, …) emit multi-line stderr, and forwarding that
/// raw into `x-amz-error-message` previously panicked the dispatcher.
fn sanitize_header_value(s: &str) -> String {
    const MAX_LEN: usize = 1024;
    let mut out = String::with_capacity(s.len().min(MAX_LEN));
    for ch in s.chars() {
        if out.len() >= MAX_LEN {
            break;
        }
        // Header values forbid CR, LF, and other control bytes (RFC 9110).
        // Replace with a single space so multi-line messages stay readable.
        if ch.is_control() {
            if !out.ends_with(' ') {
                out.push(' ');
            }
        } else {
            out.push(ch);
        }
    }
    out.trim().to_string()
}

/// Build the [`ConditionContext`] passed to the IAM evaluator for one
/// request. Populates the 10 global condition keys from the resolved
/// principal + the HTTP request. Service-specific keys are deferred to
/// a follow-up batch and left empty.
fn build_condition_context(
    principal: &Principal,
    remote_addr: Option<SocketAddr>,
    region: &str,
    secure_transport: bool,
) -> ConditionContext {
    let now = chrono::Utc::now();
    ConditionContext {
        aws_username: aws_username_from_principal(principal),
        aws_userid: Some(principal.user_id.clone()),
        aws_principal_arn: Some(principal.arn.clone()),
        aws_principal_account: Some(principal.account_id.clone()),
        aws_principal_type: Some(principal_type_label(principal.principal_type).to_string()),
        aws_source_ip: remote_addr.map(|sa| sa.ip()),
        aws_current_time: Some(now),
        aws_epoch_time: Some(now.timestamp()),
        aws_secure_transport: Some(secure_transport),
        aws_requested_region: Some(region.to_string()),
        // F3 keys: populated from the caller's session context when STS
        // mints credentials with MFA / SAML / OIDC / VPC-endpoint hints.
        // Default-None here so tests/dispatch sites that don't set them
        // safe-fail any policy referencing them — matching AWS for keys
        // that aren't asserted.
        aws_mfa_present: None,
        aws_mfa_age_seconds: None,
        aws_called_via: Vec::new(),
        aws_source_vpce: None,
        aws_source_vpc: None,
        aws_vpc_source_ip: None,
        aws_federated_provider: None,
        aws_token_issue_time: None,
        service_keys: Default::default(),
        resource_tags: None,
        request_tags: None,
        principal_tags: None,
    }
}

/// `aws:username` is only set for IAM users, matching AWS. For assumed
/// roles, federated users, root, and unknown principals the key is
/// absent — operators that reference it without `IfExists` safe-fail.
fn aws_username_from_principal(principal: &Principal) -> Option<String> {
    if principal.principal_type != PrincipalType::User {
        return None;
    }
    let after = principal.arn.rsplit_once(":user/").map(|(_, s)| s)?;
    // Strip any IAM path prefix; bare username is the last segment.
    Some(after.rsplit('/').next().unwrap_or(after).to_string())
}

/// AWS's `aws:PrincipalType` uses PascalCase identifiers, distinct from
/// the lowercase ones [`PrincipalType::as_str`] returns for ARNs.
fn principal_type_label(t: PrincipalType) -> &'static str {
    match t {
        PrincipalType::User => "User",
        PrincipalType::AssumedRole => "AssumedRole",
        PrincipalType::FederatedUser => "FederatedUser",
        PrincipalType::Root => "Account",
        PrincipalType::Unknown => "Unknown",
    }
}

/// Best-effort detection of TLS-terminated requests. Direct HTTPS
/// connections are not yet supported by the fakecloud server (it speaks
/// plain HTTP), so the only signal is an `x-forwarded-proto: https`
/// header set by an upstream proxy. Anything else evaluates to `false`,
/// which matches the typical local-dev setup.
fn is_secure_transport(headers: &http::HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("https"))
        .unwrap_or(false)
}

trait ProtocolExt {
    fn error_status(&self) -> StatusCode;
}

impl ProtocolExt for AwsProtocol {
    fn error_status(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_max_request_body_bytes_is_one_gib() {
        // Without the env override, the cap defaults to 1 GiB. The
        // public function caches via OnceLock so only the first call
        // in the process matters; we assert the constant directly.
        assert_eq!(DEFAULT_MAX_REQUEST_BODY_BYTES, 1024 * 1024 * 1024);
    }

    #[test]
    fn dispatch_config_new_defaults_to_off() {
        let cfg = DispatchConfig::new("us-east-1", "123456789012");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.account_id, "123456789012");
        assert!(!cfg.verify_sigv4);
        assert_eq!(cfg.iam_mode, IamMode::Off);
    }

    #[test]
    fn aws_username_strips_iam_path_for_users() {
        let p = Principal {
            arn: "arn:aws:iam::123456789012:user/engineering/alice".into(),
            user_id: "AIDAALICE".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        assert_eq!(aws_username_from_principal(&p), Some("alice".into()));
    }

    #[test]
    fn aws_username_unset_for_assumed_role() {
        let p = Principal {
            arn: "arn:aws:sts::123456789012:assumed-role/ops/session".into(),
            user_id: "AROAOPS:session".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        };
        assert_eq!(aws_username_from_principal(&p), None);
    }

    #[test]
    fn principal_type_label_matches_aws_casing() {
        assert_eq!(principal_type_label(PrincipalType::User), "User");
        assert_eq!(
            principal_type_label(PrincipalType::AssumedRole),
            "AssumedRole"
        );
        assert_eq!(principal_type_label(PrincipalType::Root), "Account");
    }

    #[test]
    fn build_condition_context_populates_global_keys() {
        let p = Principal {
            arn: "arn:aws:iam::123456789012:user/alice".into(),
            user_id: "AIDAALICE".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let addr: SocketAddr = "10.0.0.1:54321".parse().unwrap();
        let ctx = build_condition_context(&p, Some(addr), "us-east-1", false);
        assert_eq!(ctx.aws_username.as_deref(), Some("alice"));
        assert_eq!(ctx.aws_userid.as_deref(), Some("AIDAALICE"));
        assert_eq!(
            ctx.aws_principal_arn.as_deref(),
            Some("arn:aws:iam::123456789012:user/alice")
        );
        assert_eq!(ctx.aws_principal_account.as_deref(), Some("123456789012"));
        assert_eq!(ctx.aws_principal_type.as_deref(), Some("User"));
        assert_eq!(
            ctx.aws_source_ip.map(|i| i.to_string()).as_deref(),
            Some("10.0.0.1")
        );
        assert_eq!(ctx.aws_requested_region.as_deref(), Some("us-east-1"));
        assert_eq!(ctx.aws_secure_transport, Some(false));
        assert!(ctx.aws_current_time.is_some());
        assert!(ctx.aws_epoch_time.is_some());
    }

    #[test]
    fn is_secure_transport_reads_x_forwarded_proto() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        assert!(is_secure_transport(&headers));
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        assert!(!is_secure_transport(&headers));
        let empty = http::HeaderMap::new();
        assert!(!is_secure_transport(&empty));
    }

    #[test]
    fn parse_account_from_arn_extracts_standard_shapes() {
        assert_eq!(
            parse_account_from_arn("arn:aws:sqs:us-east-1:123456789012:queue"),
            Some("123456789012".to_string())
        );
        assert_eq!(
            parse_account_from_arn("arn:aws:iam::123456789012:user/alice"),
            Some("123456789012".to_string())
        );
    }

    #[test]
    fn parse_account_from_arn_returns_none_for_s3_empty_account() {
        // S3 ARNs have both region and account empty.
        assert_eq!(parse_account_from_arn("arn:aws:s3:::my-bucket"), None);
        assert_eq!(
            parse_account_from_arn("arn:aws:s3:::my-bucket/path/to/key"),
            None
        );
    }

    #[test]
    fn parse_account_from_arn_returns_none_for_malformed() {
        assert_eq!(parse_account_from_arn(""), None);
        assert_eq!(parse_account_from_arn("not-an-arn"), None);
        assert_eq!(parse_account_from_arn("arn:aws:sqs:us-east-1"), None);
        assert_eq!(parse_account_from_arn("arn:aws:sqs"), None);
    }

    #[test]
    fn extract_region_from_user_agent_finds_region_segment() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "user-agent",
            "aws-sdk-rust/1.0 os/linux region/eu-central-1"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_region_from_user_agent(&headers),
            Some("eu-central-1".to_string())
        );
    }

    #[test]
    fn extract_region_from_user_agent_none_without_header() {
        let headers = http::HeaderMap::new();
        assert_eq!(extract_region_from_user_agent(&headers), None);
    }

    #[test]
    fn extract_region_from_user_agent_ignores_empty_region() {
        let mut headers = http::HeaderMap::new();
        headers.insert("user-agent", "aws-sdk-java region/".parse().unwrap());
        assert_eq!(extract_region_from_user_agent(&headers), None);
    }

    #[test]
    fn extract_region_from_user_agent_none_when_no_region_marker() {
        let mut headers = http::HeaderMap::new();
        headers.insert("user-agent", "curl/7.79.1".parse().unwrap());
        assert_eq!(extract_region_from_user_agent(&headers), None);
    }

    #[test]
    fn aws_username_none_for_root() {
        let p = Principal {
            arn: "arn:aws:iam::123456789012:root".into(),
            user_id: "123456789012".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::Root,
            source_identity: None,
            tags: None,
        };
        assert_eq!(aws_username_from_principal(&p), None);
    }

    #[test]
    fn aws_username_bare_no_path() {
        let p = Principal {
            arn: "arn:aws:iam::123456789012:user/bob".into(),
            user_id: "AIDABOB".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        assert_eq!(aws_username_from_principal(&p), Some("bob".into()));
    }

    #[test]
    fn principal_type_label_covers_federated_and_unknown() {
        assert_eq!(
            principal_type_label(PrincipalType::FederatedUser),
            "FederatedUser"
        );
        assert_eq!(principal_type_label(PrincipalType::Unknown), "Unknown");
    }

    #[test]
    fn build_condition_context_marks_secure_when_flag_set() {
        let p = Principal {
            arn: "arn:aws:iam::123456789012:user/alice".into(),
            user_id: "AIDAALICE".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let ctx = build_condition_context(&p, None, "us-west-2", true);
        assert_eq!(ctx.aws_secure_transport, Some(true));
        assert!(ctx.aws_source_ip.is_none());
        assert_eq!(ctx.aws_requested_region.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn is_secure_transport_case_insensitive() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-forwarded-proto", "HTTPS".parse().unwrap());
        assert!(is_secure_transport(&headers));
    }

    #[test]
    fn is_secure_transport_non_ascii_bytes_false() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-forwarded-proto",
            http::HeaderValue::from_bytes(&[0xFF, 0xFE]).unwrap(),
        );
        assert!(!is_secure_transport(&headers));
    }

    #[test]
    fn protocol_ext_error_status_is_bad_request() {
        assert_eq!(AwsProtocol::Query.error_status(), StatusCode::BAD_REQUEST);
        assert_eq!(AwsProtocol::Json.error_status(), StatusCode::BAD_REQUEST);
        assert_eq!(AwsProtocol::Rest.error_status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            AwsProtocol::RestJson.error_status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn build_error_response_json_has_json_content_type() {
        let resp = build_error_response(
            StatusCode::BAD_REQUEST,
            "TestCode",
            "test msg",
            "req-1",
            AwsProtocol::Json,
        );
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("json"));
        let rid = resp
            .headers()
            .get("x-amzn-requestid")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(rid, "req-1");
    }

    #[test]
    fn build_error_response_rest_returns_xml_content_type() {
        let resp = build_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "bucket missing",
            "req-2",
            AwsProtocol::Rest,
        );
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("xml"));
    }

    #[test]
    fn build_error_response_query_returns_xml() {
        let resp = build_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidParameter",
            "bad param",
            "req-3",
            AwsProtocol::Query,
        );
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("xml"));
    }

    /// Regression for issue #1539: multi-line backend errors (e.g. podman
    /// stderr) used to panic the dispatcher when stuffed into the
    /// `x-amz-error-message` HTTP header. The response must build cleanly
    /// and the header value must not contain control characters.
    #[test]
    fn build_error_response_with_multiline_message_does_not_panic() {
        let resp = build_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServiceException",
            "Lambda execution failed: container failed to start: docker start failed: \
             Error: unable to start container \"abc\": \
             failed to create new hosts file:\nhost-gateway is empty\n",
            "req-multi",
            AwsProtocol::Json,
        );
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let msg = resp
            .headers()
            .get("x-amz-error-message")
            .expect("x-amz-error-message must be set even when input contains newlines")
            .to_str()
            .unwrap();
        assert!(!msg.contains('\n'));
        assert!(!msg.contains('\r'));
        assert!(msg.contains("Lambda execution failed"));
        assert!(msg.contains("host-gateway is empty"));
    }

    #[test]
    fn build_error_response_with_control_chars_strips_them() {
        let resp = build_error_response(
            StatusCode::BAD_REQUEST,
            "Code\twith\ttabs",
            "msg\x00with\x01nulls",
            "req-ctrl",
            AwsProtocol::Json,
        );
        let code = resp
            .headers()
            .get("x-amz-error-code")
            .unwrap()
            .to_str()
            .unwrap();
        let msg = resp
            .headers()
            .get("x-amz-error-message")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(!code.contains('\t'));
        assert!(!msg.contains('\x00'));
        assert!(!msg.contains('\x01'));
    }

    #[test]
    fn sanitize_header_value_truncates_long_input() {
        let huge = "x".repeat(5_000);
        let out = sanitize_header_value(&huge);
        assert!(out.len() <= 1024);
    }

    #[test]
    fn sanitize_header_value_collapses_consecutive_control_runs() {
        let out = sanitize_header_value("a\n\n\n\rb");
        assert_eq!(out, "a b");
    }

    #[test]
    fn dispatch_config_carries_opt_in_flags() {
        let cfg = DispatchConfig {
            region: "eu-west-1".to_string(),
            account_id: "000000000000".to_string(),
            verify_sigv4: true,
            iam_mode: IamMode::Strict,
            credential_resolver: None,
            policy_evaluator: None,
            resource_policy_provider: None,
            scp_resolver: None,
        };
        assert!(cfg.verify_sigv4);
        assert!(cfg.iam_mode.is_strict());
        assert!(cfg.resource_policy_provider.is_none());
        assert!(cfg.scp_resolver.is_none());
    }

    fn s3_sigv4_headers() -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fake"
                .parse()
                .unwrap(),
        );
        headers
    }

    #[test]
    fn streaming_route_path_style_s3_put_object() {
        let headers = s3_sigv4_headers();
        assert_eq!(
            streaming_route(
                &http::Method::PUT,
                "/my-bucket/key.txt",
                &headers,
                &HashMap::new(),
            ),
            Some(("s3", "")),
        );
    }

    #[test]
    fn streaming_route_path_style_create_bucket_skipped() {
        // `PUT /bucket` (no trailing key) is CreateBucket — must NOT
        // hit the streaming path.
        let headers = s3_sigv4_headers();
        assert_eq!(
            streaming_route(&http::Method::PUT, "/my-bucket", &headers, &HashMap::new(),),
            None,
        );
    }

    #[test]
    fn streaming_route_virtual_hosted_s3_put_object() {
        let mut headers = s3_sigv4_headers();
        headers.insert(
            "host",
            "vhost-bucket.s3.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        // Virtual-hosted PUT has no bucket in the URL path (`/<key>`),
        // so the slash check used for path-style would reject it. The
        // Host parser confirms this is virtual-hosted S3 and the key
        // flows through the streaming dispatch.
        assert_eq!(
            streaming_route(&http::Method::PUT, "/hello.txt", &headers, &HashMap::new(),),
            Some(("s3", "")),
        );
    }

    #[test]
    fn streaming_route_virtual_hosted_s3_root_skipped() {
        // `PUT /` against a virtual-hosted Host = CreateBucket, which
        // is handled buffered. Empty path-after-slash must short-circuit.
        let mut headers = s3_sigv4_headers();
        headers.insert(
            "host",
            "vhost-bucket.s3.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            streaming_route(&http::Method::PUT, "/", &headers, &HashMap::new()),
            None,
        );
    }

    #[test]
    fn streaming_route_ecr_blob_upload() {
        let headers = http::HeaderMap::new();
        assert_eq!(
            streaming_route(
                &http::Method::PATCH,
                "/v2/my-repo/blobs/uploads/abcd1234",
                &headers,
                &HashMap::new(),
            ),
            Some(("ecr", "")),
        );
        assert_eq!(
            streaming_route(
                &http::Method::PUT,
                "/v2/my-repo/blobs/uploads/abcd1234",
                &headers,
                &HashMap::new(),
            ),
            Some(("ecr", "")),
        );
    }

    #[test]
    fn streaming_route_presigned_v4_s3_put() {
        let headers = http::HeaderMap::new();
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Credential".to_string(),
            "test/20240101/us-east-1/s3/aws4_request".to_string(),
        );
        assert_eq!(
            streaming_route(
                &http::Method::PUT,
                "/my-bucket/key.txt",
                &headers,
                &query_params,
            ),
            Some(("s3", "")),
        );
    }

    #[test]
    fn streaming_route_non_s3_auth_header_skipped() {
        // Same path shape but the SigV4 service is lambda — must not
        // wire the streaming dispatch (Lambda has its own buffered path).
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/lambda/aws4_request, \
             SignedHeaders=host, Signature=fake"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            streaming_route(
                &http::Method::PUT,
                "/my-bucket/key.txt",
                &headers,
                &HashMap::new(),
            ),
            None,
        );
    }

    #[test]
    fn streaming_route_get_skipped() {
        let headers = s3_sigv4_headers();
        assert_eq!(
            streaming_route(
                &http::Method::GET,
                "/my-bucket/key.txt",
                &headers,
                &HashMap::new(),
            ),
            None,
        );
    }
}
