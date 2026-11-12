use axum::body::Body;
use axum::extract::{ConnectInfo, Extension, Query};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::auth::{
    is_root_bypass, ConditionContext, CredentialResolver, IamMode, IamPolicyEvaluator,
    InternalCaller, Principal, PrincipalType, ResourcePolicyProvider,
};
use crate::protocol::{self, AwsProtocol};
use crate::registry::ServiceRegistry;
use crate::service::{AwsRequest, ResponseBody};

/// Pins an in-process request to one REST service; see [`dispatch_to_service`].
/// Private, so only this module can attach it.
#[derive(Clone, Copy, Debug)]
struct PinnedService(&'static str);

/// Dispatch an in-process request straight to one REST-protocol service
/// (`"s3"`), bypassing the HTTP router and service detection.
///
/// The CloudFront data plane fetches an S3 origin this way: the request is by
/// construction an S3 request addressed to the origin bucket (via its `Host`),
/// so no viewer path (`/_fakecloud/*`, `/latest/*`, ...) can reach one of
/// fakecloud's own routes, and no viewer header (`X-Amz-Target`, an
/// `Authorization` scoped to another service, `?Action=`) can steer it to
/// another service. Authentication and IAM enforcement run exactly as for any
/// request, including an [`InternalCaller`] extension. The source address is
/// the request's `ConnectInfo<SocketAddr>` extension, or loopback.
pub async fn dispatch_to_service(
    service: &'static str,
    registry: Arc<ServiceRegistry>,
    config: Arc<DispatchConfig>,
    mut request: Request<Body>,
) -> Response<Body> {
    let remote_addr = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
    let query = match Query::<HashMap<String, String>>::try_from_uri(request.uri()) {
        Ok(q) => q,
        Err(e) => {
            return build_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                &format!("Invalid query string: {e}"),
                &uuid::Uuid::new_v4().to_string(),
                AwsProtocol::Rest,
            )
        }
    };
    request.extensions_mut().insert(PinnedService(service));
    dispatch(
        ConnectInfo(remote_addr),
        Extension(registry),
        Extension(config),
        query,
        request,
    )
    .await
}

/// Services whose Smithy model marks operations `@requestCompression`, so a
/// client may send a gzip `Content-Encoding` body that the service decodes.
/// Elsewhere `Content-Encoding` is left alone: S3 stores it as object metadata
/// and API Gateway forwards it to the backend.
const REQUEST_COMPRESSION_SERVICES: &[&str] = &["monitoring"];

/// Decompressed bodies are capped like buffered ones, so a small gzip bomb
/// can't expand past the request size limit.
fn decode_request_compression(
    headers: &http::HeaderMap,
    rpc_v2_cbor: Option<&protocol::DetectedRequest>,
    body: Bytes,
) -> Result<Bytes, (String, AwsProtocol)> {
    let gzipped = headers
        .get_all(http::header::CONTENT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|enc| enc.trim().eq_ignore_ascii_case("gzip"));
    if !gzipped || body.is_empty() {
        return Ok(body);
    }
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(protocol::parse_amz_target);
    let (service, protocol) = if let Some(d) = rpc_v2_cbor {
        (Some(d.service.clone()), AwsProtocol::RpcV2Cbor)
    } else if let Some(d) = target {
        (Some(d.service), AwsProtocol::Json)
    } else {
        (
            protocol::extract_service_from_auth(headers),
            AwsProtocol::Query,
        )
    };
    if !service.is_some_and(|s| REQUEST_COMPRESSION_SERVICES.contains(&s.as_str())) {
        return Ok(body);
    }
    use std::io::Read;
    let limit = max_request_body_bytes() as u64;
    let mut out = Vec::new();
    let read = flate2::read::MultiGzDecoder::new(body.as_ref())
        .take(limit + 1)
        .read_to_end(&mut out);
    match read {
        Ok(_) if out.len() as u64 > limit => {
            Err(("Decompressed request body too large".to_string(), protocol))
        }
        Ok(_) => Ok(Bytes::from(out)),
        Err(e) => Err((
            format!("Unable to decompress gzip request body: {e}"),
            protocol,
        )),
    }
}

/// awsJson content type of protocol version 1.0.
const AWS_JSON_1_0: &str = "application/x-amz-json-1.0";
/// awsJson content type of protocol version 1.1, the services' default.
const AWS_JSON_1_1: &str = "application/x-amz-json-1.1";

/// The main dispatch handler. All HTTP requests come through here.
pub async fn dispatch(
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    Extension(registry): Extension<Arc<ServiceRegistry>>,
    Extension(config): Extension<Arc<DispatchConfig>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Response<Body> {
    let json_1_0 = request
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.trim().eq_ignore_ascii_case(AWS_JSON_1_0));
    let mut response = dispatch_inner(remote_addr, registry, config, query_params, request).await;
    if json_1_0 {
        answer_in_json_1_0(&mut response);
    }
    response
}

/// An awsJson 1.0 service (DynamoDB, SQS, CloudWatch, Step Functions, ...)
/// answers in the content type of its protocol version, which its clients
/// send on the request. Handlers render JSON as 1.1, so relabel the response
/// (success or error) for a 1.0 caller.
fn answer_in_json_1_0(response: &mut Response<Body>) {
    let is_1_1 = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.eq_ignore_ascii_case(AWS_JSON_1_1));
    if is_1_1 {
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static(AWS_JSON_1_0),
        );
    }
}

async fn dispatch_inner(
    remote_addr: SocketAddr,
    registry: Arc<ServiceRegistry>,
    config: Arc<DispatchConfig>,
    query_params: HashMap<String, String>,
    request: Request<Body>,
) -> Response<Body> {
    let remote_addr = Some(remote_addr);
    let request_id = uuid::Uuid::new_v4().to_string();

    let (mut parts, body) = request.into_parts();
    // Set only by [`dispatch_to_service`]: the request is for this service,
    // whatever its headers or query say.
    let pinned = parts.extensions.get::<PinnedService>().map(|p| p.0);

    // Streaming opt-in: if the route is a known large-body S3 / ECR
    // upload, we skip the buffered `to_bytes` step entirely and hand
    // the raw body to the service handler. The handler spills it to
    // disk on the fly. Header-only detection covers every streaming
    // candidate (none of them rely on form-body sniffing).
    // Smithy RPC v2 CBOR is recognized by its `smithy-protocol` header and
    // `/service/{Service}/operation/{Op}` path before any other detection:
    // its SigV4 scope alone would otherwise route it as the service's Query
    // protocol.
    let rpc_v2_cbor = if pinned.is_none() {
        protocol::detect_rpc_v2_cbor(&parts.headers, parts.uri.path())
    } else {
        None
    };
    let stream_route = streaming_route(
        &parts.method,
        parts.uri.path(),
        &parts.headers,
        &query_params,
    );
    let header_only = protocol::detect_service_headers_only(&parts.headers, &query_params);
    let stream_dispatch = match (&stream_route, &header_only) {
        // A pinned request is always buffered: its caller already holds the
        // whole body, and header detection must not pick its service.
        _ if pinned.is_some() => None,
        _ if rpc_v2_cbor.is_some() => None,
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
        // An unsigned object upload S3 will serve -- on an S3 endpoint host,
        // or path-style on the shared listener (routed below to S3 for an
        // existing bucket or one API Gateway does not claim) -- streams like
        // a signed one; buffering it left PutObject without its body stream.
        (None, header_only)
            if is_unsigned(&parts.headers, &query_params)
                && is_s3_object_put(&parts.method, parts.uri.path(), &parts.headers)
                && !protocol::is_s3_control_host(&parts.headers)
                && match header_only {
                    Some(d) => d.service == "s3",
                    None => {
                        !parts.uri.path().starts_with("/v2/")
                            && (anonymous_s3_bucket(&parts.uri, &config).is_some()
                                || unsigned_request_is_s3_path_style(
                                    &parts.method,
                                    &parts.uri,
                                    &parts.headers,
                                    &query_params,
                                    &registry,
                                    &config,
                                ))
                    }
                } =>
        {
            Some(protocol::DetectedRequest {
                service: "s3".to_string(),
                action: String::new(),
                protocol: AwsProtocol::Rest,
            })
        }
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

    // `@requestCompression`: a client may gzip the body of an operation whose
    // model allows it. Decode it before anything parses the body (Query
    // detection reads the form body), but keep the wire bytes for SigV4,
    // which signed the compressed payload.
    let wire_body = body_bytes.clone();
    let body_bytes = if pinned.is_none() && stream_dispatch.is_none() {
        match decode_request_compression(&parts.headers, rpc_v2_cbor.as_ref(), body_bytes) {
            Ok(b) => b,
            Err((message, protocol)) => {
                return build_error_response(
                    StatusCode::BAD_REQUEST,
                    "SerializationException",
                    &message,
                    &request_id,
                    protocol,
                );
            }
        }
    } else {
        body_bytes
    };

    // Detect service and action
    let detected = if let Some(service) = pinned {
        protocol::DetectedRequest {
            service: service.to_string(),
            action: String::new(),
            protocol: AwsProtocol::Rest,
        }
    } else if let Some(d) = rpc_v2_cbor {
        d
    } else if let Some(d) = stream_dispatch {
        d
    } else {
        match protocol::detect_service(&parts.headers, &query_params, &body_bytes) {
            Some(d) => d,
            None => {
                // A request carrying X-Amz-Target is unambiguously an awsJson
                // call whose operation we couldn't map to a known service. AWS
                // answers these with UnknownOperationException; routing them to
                // the apigateway catch-all below would 404 with a misleading
                // "Stage not found" instead.
                if let Some(target) = parts
                    .headers
                    .get("x-amz-target")
                    .and_then(|v| v.to_str().ok())
                {
                    return build_error_response(
                        StatusCode::BAD_REQUEST,
                        "UnknownOperationException",
                        &format!("The operation {target} is not recognized."),
                        &request_id,
                        AwsProtocol::Json,
                    );
                }
                // OPTIONS requests (CORS preflight) don't carry Authorization headers.
                // One addressed to an API's `{api-id}.execute-api.` host is that
                // API's preflight (its OPTIONS method, e.g. a MOCK CORS
                // integration); any other unsigned OPTIONS goes to S3, the other
                // REST service that answers CORS preflights.
                if parts.method == http::Method::OPTIONS && is_execute_api_host(&parts.headers) {
                    protocol::DetectedRequest {
                        service: "apigateway".to_string(),
                        action: String::new(),
                        protocol: AwsProtocol::RestJson,
                    }
                } else if parts.method == http::Method::OPTIONS {
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
                } else if let Some(bucket) = anonymous_s3_bucket(&parts.uri, &config) {
                    // Unsigned request whose first path segment names an
                    // existing S3 bucket: an anonymous path-style S3 access
                    // (e.g. serving a public-read object to a browser). Without
                    // this it would fall through to the apigateway catch-all
                    // below and 404 with "Stage not found" (#1707). Authorization
                    // for the anonymous caller still runs in the IAM block.
                    tracing::debug!(bucket = %bucket, "routing unsigned request to S3 (existing bucket)");
                    protocol::DetectedRequest {
                        service: "s3".to_string(),
                        action: String::new(),
                        protocol: AwsProtocol::Rest,
                    }
                } else if unsigned_request_is_s3_path_style(
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    &query_params,
                    &registry,
                    &config,
                ) {
                    // Unsigned path-style request on the shared listener that
                    // no API Gateway data plane claims: on AWS it is an S3
                    // request to S3's endpoint, for an existing bucket or not
                    // (`NoSuchBucket`), so S3 answers it rather than the
                    // apigateway catch-all below ("Stage not found").
                    protocol::DetectedRequest {
                        service: "s3".to_string(),
                        action: String::new(),
                        protocol: AwsProtocol::Rest,
                    }
                } else if !parts.uri.path().starts_with("/_")
                    || parts.uri.path().starts_with("/_aws/execute-api/")
                {
                    // API Gateway execute API calls: an unsigned request the
                    // API Gateway data plane claimed above, or a signed one
                    // whose signing scope names no REST service (an IAM-auth
                    // API signed for `execute-api`). Route them to the
                    // apigateway service, which validates that a matching
                    // API/stage exists. Skip special FakeCloud endpoints (/_*),
                    // except LocalStack's path-style execute-api invocation URL
                    // `/_aws/execute-api/{api-id}/{stage}/{path}`.
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
        match bedrock_agent_service_for(&parts.method, parts.uri.path()) {
            Some(service) => protocol::DetectedRequest {
                service: service.to_string(),
                ..detected
            },
            None => detected,
        }
    } else {
        detected
    };

    // Amazon DocumentDB shares RDS's Query wire protocol, `rds` SigV4
    // signing scope, and `rds.<region>.amazonaws.com` endpoint, so a real
    // `aws-sdk-docdb` request is indistinguishable from `aws-sdk-rds` by
    // signing name or host alone. The DocumentDB SDK does stamp an
    // `api/docdb` token into its `user-agent`; use it to route the request
    // to the dedicated `docdb` handler. (The conformance probe signs the
    // `docdb` scope directly, so it never reaches this branch.) Requests
    // without the token stay on `rds`.
    let detected = if detected.service == "rds" && user_agent_indicates_docdb(&parts.headers) {
        protocol::DetectedRequest {
            service: "docdb".to_string(),
            ..detected
        }
    } else {
        detected
    };

    // Amazon Neptune shares RDS's Query wire protocol, `rds` SigV4 signing
    // scope, and `rds.<region>.amazonaws.com` endpoint, so a real
    // `aws-sdk-neptune` request is indistinguishable from `aws-sdk-rds` by
    // signing name or host alone. The Neptune SDK does stamp an
    // `api/neptune` token into its `user-agent`; use it to route the request
    // to the dedicated `neptune` handler. (The conformance probe signs the
    // `neptune` scope directly, so it never reaches this branch.) Requests
    // without the token stay on `rds`.
    let detected = if detected.service == "rds" && user_agent_indicates_neptune(&parts.headers) {
        protocol::DetectedRequest {
            service: "neptune".to_string(),
            ..detected
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
                ErrorEnvelope::for_request(&detected, &parts.headers),
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
    // SigV2 presigned URLs (`AWSAccessKeyId` + `Signature` + `Expires` query
    // parameters) carry the access key outside the SigV4 grammar, so the SigV4
    // parsers above return None. Recover the key here so a SigV2-presigned
    // request is attributed to its caller instead of being treated as
    // anonymous (which would deny it under the object read auth gate).
    let access_key_id = sigv4_info
        .as_ref()
        .map(|info| info.access_key.clone())
        .or_else(|| sigv2_presigned_access_key(&query_params));

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
    //
    // A fully anonymous request — no `Authorization` header AND no presigned
    // credential (SigV4 `X-Amz-Credential` or SigV2 `AWSAccessKeyId`) — carries
    // no signature to verify, so it must NOT be hard-403'd here even under
    // `--verify-sigv4`. AWS treats an unsigned request as the anonymous
    // principal and lets a resource policy / public-read ACL authorize it
    // (e.g. a public S3 object GET). Let it fall through to the
    // anonymous-authorization path below, which allows a public resource and
    // otherwise denies. A request that DID present a credential but failed to
    // parse is NOT anonymous (`Authorization` non-empty or a presign query
    // present) and still returns the malformed-signature error below.
    let is_fully_anonymous = auth_header.is_empty()
        && !query_params.contains_key("X-Amz-Credential")
        && sigv2_presigned_access_key(&query_params).is_none();
    if config.verify_sigv4
        && !is_fully_anonymous
        && !is_root_bypass(caller_akid)
        && config.credential_resolver.is_some()
    {
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
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
        };
        let resolved_for_verify = match resolved.as_ref() {
            Some(r) => r,
            None => {
                return unresolved_credential_response(
                    &config,
                    caller_akid,
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
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
            body: &wire_body,
        };
        match fakecloud_aws::sigv4::verify(
            &parsed,
            &verify_req,
            &resolved_for_verify.secret_access_key,
            chrono::Utc::now(),
        ) {
            Ok(()) => {
                // Bind the buffered request body to the signed
                // `x-amz-content-sha256`. The sigv4 canonical builder uses that
                // header value verbatim (it is a signed header) and deliberately
                // does NOT re-hash the body: for streaming / aws-chunked routes
                // `body_bytes` is empty at that layer, so re-hashing there would
                // reject legitimate signed requests (see the
                // `feedback_sigv4_body_hash_wrong_layer` caveat). S3 rebinds the
                // body hash in its own write path (`XAmzContentSHA256Mismatch`);
                // every other service is bound HERE, where the full buffered body
                // is available. Only a genuine 64-char lowercase-hex digest is
                // checked — `UNSIGNED-PAYLOAD`, `STREAMING-*`, and presigned
                // (UNSIGNED-PAYLOAD) requests are skipped, so correct clients are
                // unaffected. A mismatch means the body was altered after signing;
                // AWS would then compute a different canonical request, so return
                // `SignatureDoesNotMatch`.
                if !parsed.is_presigned && detected.service != "s3" {
                    if let Some(signed_hash) = parts
                        .headers
                        .get("x-amz-content-sha256")
                        .and_then(|v| v.to_str().ok())
                        .filter(|h| is_hex_sha256(h))
                    {
                        if sha256_hex_lower(&wire_body) != signed_hash {
                            return build_error_response(
                                StatusCode::FORBIDDEN,
                                "SignatureDoesNotMatch",
                                "The request signature we calculated does not match the signature you provided",
                                &request_id,
                                ErrorEnvelope::for_request(&detected, &parts.headers),
                            );
                        }
                    }
                }
            }
            Err(fakecloud_aws::sigv4::SigV4Error::RequestTimeTooSkewed { .. }) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "RequestTimeTooSkewed",
                    "The difference between the request time and the current time is too large",
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::InvalidDate(msg)) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "IncompleteSignature",
                    &format!("Invalid x-amz-date: {msg}"),
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::Malformed(msg)) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "IncompleteSignature",
                    &format!("Malformed SigV4 signature: {msg}"),
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::SignatureMismatch) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "SignatureDoesNotMatch",
                    "The request signature we calculated does not match the signature you provided",
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::PresignedUrlExpired { .. }) => {
                return build_error_response(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "Request has expired",
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
            Err(fakecloud_aws::sigv4::SigV4Error::InvalidPresignExpires(_)) => {
                return build_error_response(
                    StatusCode::BAD_REQUEST,
                    "AuthorizationQueryParametersError",
                    "X-Amz-Expires must be a number between 1 and 604800 seconds",
                    &request_id,
                    ErrorEnvelope::for_request(&detected, &parts.headers),
                );
            }
        }
    }

    // A SigV4 presigned S3 URL can carry request headers as query parameters
    // (the JS v3 presigner hoists every `x-amz-*` header this way). S3 applies
    // them as if they were headers, so lift them into the header map now that
    // the signature over the raw query has been verified.
    if detected.service == "s3" && query_params.contains_key("X-Amz-Credential") {
        hoist_presigned_query_headers(&mut parts.headers, &query_params);
    }

    // Build path segments. For S3 virtual-hosted-style requests the bucket
    // lives in the Host header, not the path — prepend it so the S3 handler
    // sees a uniform path-style request. SigV4 verification above already
    // ran against the wire path, so this rewrite is signature-safe.
    let wire_path = parts.uri.path();
    let path = if detected.service == "s3" {
        s3_routing_path(
            wire_path,
            host_info.as_ref().and_then(|h| h.bucket.as_deref()),
        )
    } else {
        wire_path.to_string()
    };
    let raw_query = parts.uri.query().unwrap_or("").to_string();
    // Split on `/` first, then percent-decode each segment once, so handlers
    // see decoded `@httpLabel` values and an encoded `/` (`%2F`) stays inside
    // its label. `raw_path` keeps the undecoded wire form.
    let path_segments = crate::path::split_path_segments(&path);

    // rpcv2Cbor: the signature above covered the CBOR wire body; from here on
    // the request is handled on the service's JSON path, so swap in the
    // equivalent awsJson document.
    let body_bytes = if detected.protocol == AwsProtocol::RpcV2Cbor {
        match crate::cbor::decode_to_json(&body_bytes) {
            Ok(json) => Bytes::from(json.to_string()),
            Err(e) => {
                return build_error_response(
                    StatusCode::BAD_REQUEST,
                    "SerializationException",
                    &format!("Unable to decode CBOR request body: {e}"),
                    &request_id,
                    AwsProtocol::RpcV2Cbor,
                );
            }
        }
    } else {
        body_bytes
    };

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

    // Merge query params with form body params for the Query family (awsQuery
    // and ec2Query share identical form-encoded request bodies).
    let mut all_params = query_params;
    if matches!(
        detected.protocol,
        AwsProtocol::Query | AwsProtocol::Ec2Query
    ) {
        let body_params = protocol::parse_query_body(&body_bytes);
        for (k, v) in body_params {
            all_params.entry(k).or_insert(v);
        }
    }

    // CloudWatch (`monitoring`) advertises awsJson1_0 alongside awsQuery. Its
    // handlers all read the flat awsQuery param map, so when a client uses the
    // JSON protocol we flatten the JSON body into that same map, leaving the
    // handlers unchanged. The handler emits a JSON response for JSON callers.
    if matches!(
        detected.protocol,
        AwsProtocol::Json | AwsProtocol::RpcV2Cbor
    ) && detected.service == "monitoring"
    {
        let body_params = protocol::flatten_json_to_query(&body_bytes);
        for (k, v) in body_params {
            all_params.entry(k).or_insert(v);
        }
    }

    // An in-process request fakecloud issues as an AWS-owned principal (e.g.
    // CloudFront fetching an S3 origin through an origin access control). The
    // identity rides a request extension that only in-process code can set,
    // and is honored only when the request presents no credentials of its
    // own: a request that does is authorized as that caller instead.
    let internal_caller = if is_fully_anonymous && access_key_id.is_none() {
        parts.extensions.get::<InternalCaller>().cloned()
    } else {
        None
    };
    let caller_principal =
        caller_principal.or_else(|| internal_caller.as_ref().map(|c| c.principal()));

    let error_envelope = ErrorEnvelope::for_request(&detected, &parts.headers);
    let aws_request = AwsRequest {
        service: detected.service.clone(),
        action: detected.action.clone(),
        region,
        // A service acting for a customer works on the target resource in its
        // owner's account: an S3 origin bucket may belong to another account
        // than the distribution fetching it.
        account_id: match internal_caller.as_ref() {
            Some(caller) => {
                internal_caller_account(caller, &detected.service, &path_segments, &config)
            }
            None => caller_principal
                .as_ref()
                .map(|p| p.account_id.clone())
                .unwrap_or_else(|| config.account_id.clone()),
        },
        request_id: request_id.clone(),
        headers: parts.headers,
        query_params: all_params,
        body: body_bytes,
        body_stream: parking_lot::Mutex::new(body_stream),
        path_segments,
        raw_path: path,
        raw_query,
        method: parts.method,
        is_query_protocol: matches!(
            detected.protocol,
            AwsProtocol::Query | AwsProtocol::Ec2Query
        ),
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
    // handler so a deny never reaches business logic. `test*` bypass AKIDs
    // are exempt; the account's IAM root needs no identity policy, as in
    // AWS, but stays capped by its SCPs and, across accounts, by the
    // resource's policy (`authorize_root`). Services that haven't opted in via
    // `iam_enforceable()` are transparently skipped — the startup log
    // lists which services are under enforcement so users always know.
    if config.iam_mode.is_enabled()
        && service.iam_enforceable()
        && !is_root_bypass(aws_request.access_key_id.as_deref().unwrap_or(""))
    {
        if let Some(evaluator) = config.policy_evaluator.as_ref() {
            if let Some(caller) = internal_caller.as_ref() {
                if let Some(denied) = authorize_internal_caller(
                    caller,
                    service.as_ref(),
                    &aws_request,
                    evaluator.as_ref(),
                    &config,
                    &detected,
                    &request_id,
                ) {
                    return denied;
                }
            } else if let Some(principal) = aws_request.principal.as_ref() {
                if principal.is_root() {
                    if let Some(denied) = authorize_root(
                        principal,
                        service.as_ref(),
                        &aws_request,
                        evaluator.as_ref(),
                        &config,
                        &detected,
                        &request_id,
                        remote_addr,
                        resolved.as_ref(),
                    ) {
                        return denied;
                    }
                } else {
                    // A request can need several authorizations -- one per
                    // table in a batch, say -- and every one must allow it.
                    let iam_actions = service.iam_actions_for(&aws_request);
                    if !iam_actions.is_empty() {
                        for iam_action in &iam_actions {
                            let mut condition_context = principal_condition_context(
                                principal,
                                resolved.as_ref(),
                                service.as_ref(),
                                &aws_request,
                                iam_action,
                                &detected,
                                remote_addr,
                            );
                            let service_resource = !iam_action.is_pass_role();

                            // Phase 2: fetch the resource-based policy (if
                            // any) attached to the target resource and
                            // pass it to the evaluator alongside the
                            // principal's identity policies. The resource's
                            // owning account is parsed from the ARN (#381
                            // multi-account alignment); S3 ARNs have an
                            // empty account field, so we fall back to the
                            // server's configured account ID in that case.
                            // A request creating its resource in the caller's
                            // account (S3 CreateBucket) is same-account, whatever
                            // account holds that name today.
                            let in_caller_account =
                                service.iam_resource_in_caller_account(&aws_request);
                            let resource_policy_json = config
                                .resource_policy_provider
                                .as_ref()
                                .filter(|_| service_resource && !in_caller_account)
                                .filter(|p| {
                                    p.resource_policy_applies(&detected.service, iam_action)
                                })
                                .and_then(|p| {
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
                                .filter(|_| service_resource && !in_caller_account)
                                .and_then(|p| {
                                    p.resource_owner_account(
                                        &detected.service,
                                        &iam_action.resource,
                                    )
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
                            // Global keys AWS sets on every request, which
                            // perimeter guardrails gate on (`Deny ...
                            // StringNotEquals aws:PrincipalOrgID` /
                            // `aws:ResourceAccount`): absent, those negated
                            // conditions would deny everyone.
                            add_global_request_keys(
                                &mut condition_context,
                                principal,
                                &resource_account_id,
                                config.scp_resolver.as_deref(),
                            );
                            let decision = evaluator.evaluate_with_resource_policy(
                                principal,
                                iam_action,
                                &condition_context,
                                resource_policy_json.as_deref(),
                                &resource_account_id,
                                &caller_session_policies,
                                scps.as_deref(),
                            );
                            // A grant on the resource (KMS) only adds
                            // permission: it is consulted when the policies
                            // left the request undecided, never over an
                            // explicit Deny.
                            let decision = if decision == crate::auth::IamDecision::ImplicitDeny
                                && service_resource
                                && !in_caller_account
                                && grant_authorizes(
                                    config.resource_policy_provider.as_deref(),
                                    evaluator.as_ref(),
                                    &detected.service,
                                    &aws_request,
                                    iam_action,
                                    principal,
                                    &condition_context,
                                    &caller_session_policies,
                                    scps.as_deref(),
                                ) {
                                crate::auth::IamDecision::Allow
                            } else {
                                decision
                            };
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
                                        "aws:PrincipalArn": condition_context
                                            .aws_principal_arn
                                            .clone()
                                            .unwrap_or_else(|| principal.arn.clone()),
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
                                    // When the SCPs are what refused the
                                    // request, AWS says so.
                                    let scp_reason = scps.as_deref().and_then(|scps| {
                                        let ceiling = evaluator.evaluate_ceiling(
                                            principal,
                                            iam_action,
                                            &condition_context,
                                            &[],
                                            Some(scps),
                                            false,
                                        );
                                        scp_cause(decision, ceiling)
                                            .and_then(|d| scp_denial_reason(d, &action_string))
                                    });
                                    return access_denied_response(
                                        &not_authorized_message(
                                            &principal.arn,
                                            &action_string,
                                            &iam_action.resource,
                                            scp_reason.as_deref(),
                                            Some(&encoded),
                                        ),
                                        &request_id,
                                        &detected,
                                        &aws_request.headers,
                                    );
                                }
                                // Soft mode: audit log already emitted; fall
                                // through to the handler.
                            }
                        }
                    } else {
                        // Service opted in via `iam_enforceable()` but its
                        // `iam_action_for` returned no `IamAction` for this
                        // specific operation (e.g. S3's `s3_detect_action`
                        // has `_ => return None` arms for unrecognized
                        // sub-resources). Under strict enforcement that must
                        // fail closed: an operation we cannot map to an IAM
                        // action cannot be authorized, so serving it would be
                        // a fail-open bypass of `--iam strict`. Deny by
                        // default. In soft mode we preserve the historical
                        // warn-and-allow so an incomplete mapping surfaces
                        // during rollout without blocking traffic.
                        tracing::warn!(
                            target: "fakecloud::iam::audit",
                            service = %detected.service,
                            action = %aws_request.action,
                            mode = %config.iam_mode,
                            request_id = %request_id,
                            "service is iam_enforceable but has no IamAction mapping for this action; denying under strict, allowing under soft"
                        );
                        if config.iam_mode.is_strict() {
                            return unmapped_action_denial(
                                principal,
                                &aws_request,
                                &request_id,
                                &detected,
                            );
                        }
                        // Soft mode: audit log emitted; fall through to the
                        // handler.
                    }
                }
            } else if let Some(akid) = aws_request
                .access_key_id
                .as_deref()
                .filter(|_| aws_request.principal.is_none())
            {
                // The request presented an access key that resolves to no
                // identity: unknown, deactivated (Inactive), or an expired /
                // revoked STS session. AWS rejects such a request before
                // authorization (`InvalidClientTokenId` / `ExpiredToken`), so
                // under strict enforcement it must not run unchecked -- nor
                // reach STS, which would otherwise read the missing principal
                // as the account root and mint real credentials. The
                // explicit exemptions stay: the reserved `test*` root
                // identity (excluded above) and truly anonymous requests
                // (next branch). Soft mode logs and lets it through.
                tracing::warn!(
                    target: "fakecloud::iam::audit",
                    service = %detected.service,
                    action = %aws_request.action,
                    mode = %config.iam_mode,
                    request_id = %request_id,
                    "request credential does not resolve to an identity; denying under strict, allowing under soft"
                );
                if config.iam_mode.is_strict() {
                    return unresolved_credential_response(
                        &config,
                        akid,
                        &request_id,
                        ErrorEnvelope::for_request(&detected, &aws_request.headers),
                    );
                }
            } else if aws_request.access_key_id.is_none() {
                // Truly anonymous (unsigned) caller — no Authorization header at
                // all. No identity policies exist, so authorization rests
                // entirely on the resource policy (a bucket policy granting
                // `Principal:"*"`) and public-read ACLs — mirroring AWS, which
                // denies anonymous requests unless the resource is explicitly
                // made public. Without this an anonymous request that reached an
                // iam_enforceable service in enforcement mode would bypass
                // authorization entirely.
                //
                // A request whose credential did not resolve (principal `None`
                // with `access_key_id` `Some`) is handled by the branch above.
                let iam_actions = service.iam_actions_for(&aws_request);
                if !iam_actions.is_empty() {
                    for iam_action in &iam_actions {
                        let now = chrono::Utc::now();
                        let mut condition_context = ConditionContext {
                            aws_source_ip: remote_addr.map(|sa| sa.ip()),
                            aws_current_time: Some(now),
                            aws_epoch_time: Some(now.timestamp()),
                            aws_secure_transport: Some(is_secure_transport(&aws_request.headers)),
                            aws_requested_region: Some(aws_request.region.clone()),
                            ..Default::default()
                        };
                        condition_context.service_keys =
                            service.iam_condition_keys_for(&aws_request, iam_action);
                        // `aws:ResourceAccount` is set for anonymous requests
                        // too: the target resource's owner.
                        if let Some(owner) = config
                            .resource_policy_provider
                            .as_ref()
                            .and_then(|p| {
                                p.resource_owner_account(&detected.service, &iam_action.resource)
                            })
                            .or_else(|| parse_account_from_arn(&iam_action.resource))
                        {
                            condition_context
                                .service_keys
                                .entry("aws:resourceaccount".to_string())
                                .or_insert_with(|| vec![owner]);
                        }
                        let resource_policy_json =
                            config.resource_policy_provider.as_ref().and_then(|p| {
                                p.resource_policy(&detected.service, &iam_action.resource)
                            });
                        let policy_decision = evaluator.evaluate_anonymous(
                            iam_action,
                            &condition_context,
                            resource_policy_json.as_deref(),
                        );
                        let policy_allows = policy_decision.is_allow();
                        // An explicit Deny in the resource policy always wins, even
                        // over a public-read ACL — matching AWS's Deny-overrides
                        // precedence. Collapsing the decision to a bool and ORing the
                        // ACL let a public ACL override an explicit anonymous Deny.
                        let policy_explicit_deny =
                            matches!(policy_decision, crate::auth::IamDecision::ExplicitDeny);
                        let acl_allows = !policy_explicit_deny
                            && config.resource_policy_provider.as_ref().is_some_and(|p| {
                                p.public_acl_allows(
                                    &detected.service,
                                    &iam_action.resource,
                                    iam_action.action,
                                )
                            });
                        if !policy_allows && !acl_allows {
                            tracing::warn!(
                                target: "fakecloud::iam::audit",
                                service = %detected.service,
                                action = %iam_action.action_string(),
                                resource = %iam_action.resource,
                                resource_policy_present = resource_policy_json.is_some(),
                                mode = %config.iam_mode,
                                request_id = %request_id,
                                "anonymous request denied: no public bucket policy or ACL grants the action"
                            );
                            if config.iam_mode.is_strict() {
                                return build_error_response(
                                    StatusCode::FORBIDDEN,
                                    "AccessDenied",
                                    "Access Denied",
                                    &request_id,
                                    error_envelope,
                                );
                            }
                            // Soft mode: audit log emitted; fall through to the handler.
                        }
                    }
                } else {
                    // Anonymous request to an iam_enforceable service whose
                    // operation has no IamAction mapping. Mirror the signed
                    // branch above: an operation we cannot map to an IAM action
                    // cannot be authorized, so serving it would be a fail-open
                    // bypass of `--iam strict`. Deny by default under strict;
                    // soft mode warns and falls through.
                    tracing::warn!(
                        target: "fakecloud::iam::audit",
                        service = %detected.service,
                        action = %aws_request.action,
                        mode = %config.iam_mode,
                        request_id = %request_id,
                        "anonymous request to iam_enforceable service has no IamAction mapping; denying under strict, allowing under soft"
                    );
                    if config.iam_mode.is_strict() {
                        return build_error_response(
                            StatusCode::FORBIDDEN,
                            "AccessDenied",
                            "Access Denied",
                            &request_id,
                            error_envelope,
                        );
                    }
                }
            }
        }
    }

    match service.handle(aws_request).await {
        Ok(resp) => {
            let resp = if detected.protocol == AwsProtocol::RpcV2Cbor {
                rpc_v2_cbor_response(resp)
            } else {
                resp
            };
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
                error_envelope,
                err.extra_fields(),
            );
            for (k, v) in &error_headers {
                if let (Ok(name), Ok(val)) = (
                    k.parse::<http::header::HeaderName>(),
                    v.parse::<http::header::HeaderValue>(),
                ) {
                    // `Vary` combines, so two entries for it must both survive
                    // (S3 adds its CORS value to an error that may carry its
                    // own) — but an identical value is not worth repeating.
                    // Everything else replaces, so a service can still override
                    // a header this builder already set.
                    if name == http::header::VARY {
                        let already = resp
                            .headers()
                            .get_all(&name)
                            .iter()
                            .any(|existing| existing == val);
                        if !already {
                            resp.headers_mut().append(name, val);
                        }
                    } else {
                        resp.headers_mut().insert(name, val);
                    }
                }
            }
            resp
        }
    }
}

/// Frame a service response for an rpcv2Cbor caller. A service that renders
/// its own CBOR (content type `application/cbor`) passes through; a JSON body
/// is transcoded schema-lessly. Either way the response carries the
/// `smithy-protocol` header the client checks.
fn rpc_v2_cbor_response(mut resp: crate::service::AwsResponse) -> crate::service::AwsResponse {
    if resp.content_type != crate::cbor::CBOR_CONTENT_TYPE {
        if let ResponseBody::Bytes(bytes) = &resp.body {
            let json = if bytes.is_empty() {
                serde_json::Value::Object(serde_json::Map::new())
            } else {
                serde_json::from_slice(bytes).unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "non-JSON response body for an rpcv2Cbor request");
                    serde_json::Value::Object(serde_json::Map::new())
                })
            };
            resp.body = ResponseBody::Bytes(Bytes::from(crate::cbor::encode(
                &crate::cbor::json_to_cbor(&json),
            )));
            resp.content_type = crate::cbor::CBOR_CONTENT_TYPE.to_string();
        }
    }
    resp.headers.insert(
        http::HeaderName::from_static(crate::cbor::SMITHY_PROTOCOL_HEADER),
        http::HeaderValue::from_static(crate::cbor::RPC_V2_CBOR),
    );
    resp
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

/// The path an S3 request is routed on: for virtual-hosted-style the bucket
/// comes from the Host header and is prepended, unless the client already put
/// it in the path (`PUT /<bucket>` / `PUT /<bucket>/key` against a
/// virtual-hosted host). Path-style requests pass through unchanged.
///
/// Shared with [`streaming_route`] on purpose. The streaming gate has to agree
/// with routing about where the bucket ends and the key begins; when the two
/// derived it separately they drifted, and a request the router read as a
/// bucket-level operation was dispatched unbuffered, so the handler and IAM
/// enforcement saw an empty body.
///
/// Under virtual-hosted addressing the whole wire path is the object key, as
/// on real S3, so the bucket is always prefixed: on
/// `docs.s3.<region>.amazonaws.com`, `GET /docs/intro.html` addresses the key
/// `docs/intro.html`, not `intro.html`.
fn s3_routing_path(wire_path: &str, host_bucket: Option<&str>) -> String {
    let Some(bucket) = host_bucket else {
        return wire_path.to_string();
    };
    if wire_path == "/" || wire_path.is_empty() {
        format!("/{bucket}")
    } else {
        format!("/{bucket}{wire_path}")
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
    if is_s3_object_put(method, path, headers) {
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

/// Whether a request, if it is S3's, is an object-level `PUT` (PutObject,
/// UploadPart, UploadPartCopy, ...): one that streams its body.
///
/// An object upload needs a bucket AND a key. Resolve the request the way
/// routing will (bucket from the Host header for virtual-hosted, straight from
/// the path otherwise), then split it the way the request builder builds
/// `path_segments` -- on '/', dropping empty segments. Anything that leaves no
/// key is a bucket-level operation: `PUT /<bucket>`, `PUT /<bucket>/` (the form
/// the AWS SDKs actually send for CreateBucket), `PUT /<bucket>//`, and the
/// virtual-hosted equivalents. Streaming those left the body unbuffered, so
/// everything that runs before the handler -- IAM enforcement above all -- saw
/// an empty body and could not read `CreateBucketConfiguration` (no
/// `aws:RequestTag` from a create-time tag set, for one).
fn is_s3_object_put(method: &http::Method, path: &str, headers: &http::HeaderMap) -> bool {
    if method != http::Method::PUT {
        return false;
    }
    let host_bucket = protocol::parse_routing_host_from_headers(headers)
        .filter(|h| h.service == "s3")
        .and_then(|h| h.bucket);
    let routed = s3_routing_path(path, host_bucket.as_deref());
    routed.split('/').filter(|seg| !seg.is_empty()).count() >= 2
}

/// Whether the request carries no credential at all: no `Authorization`
/// header and no SigV4 or SigV2 query-authentication parameter, not even an
/// incomplete set (`AWSAccessKeyId` + `Signature` without `Expires` is a
/// broken presign, not an anonymous request).
fn is_unsigned(headers: &http::HeaderMap, query_params: &HashMap<String, String>) -> bool {
    const QUERY_AUTH: &[&str] = &[
        "X-Amz-Credential",
        "X-Amz-Signature",
        "AWSAccessKeyId",
        "Signature",
    ];
    headers
        .get(http::header::AUTHORIZATION)
        .is_none_or(|v| v.as_bytes().is_empty())
        && !QUERY_AUTH.iter().any(|k| query_params.contains_key(*k))
}

/// The SigV4 query-authentication parameters themselves. They authenticate
/// a presigned request and are not request headers.
const PRESIGN_AUTH_PARAMS: &[&str] = &[
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-date",
    "x-amz-expires",
    "x-amz-signedheaders",
    "x-amz-signature",
    "x-amz-security-token",
];

/// Copy the `x-amz-*` request headers a presigned URL carries in its query
/// string into `headers`, skipping the SigV4 auth parameters. A header the
/// client also sent directly wins over the query parameter. A non-ASCII user
/// metadata value is RFC 2047-encoded so S3 stores it as the UTF-8 the
/// percent-encoded query string meant, not as ISO-8859-1 header bytes.
fn hoist_presigned_query_headers(
    headers: &mut http::HeaderMap,
    query_params: &HashMap<String, String>,
) {
    for (key, value) in query_params {
        let lower = key.to_ascii_lowercase();
        if !lower.starts_with("x-amz-") || PRESIGN_AUTH_PARAMS.contains(&lower.as_str()) {
            continue;
        }
        let Ok(name) = http::HeaderName::from_bytes(lower.as_bytes()) else {
            continue;
        };
        if headers.contains_key(&name) {
            continue;
        }
        let value = if lower.starts_with("x-amz-meta-") {
            crate::rfc2047::encode(value)
        } else {
            value.clone()
        };
        if let Ok(value) = http::HeaderValue::from_str(&value) {
            headers.insert(name, value);
        }
    }
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

/// The server-wide buffered request body cap in bytes, from
/// `FAKECLOUD_MAX_REQUEST_BODY_BYTES` (default 1 GiB). Public so buffered
/// sub-proxies (e.g. the CloudFront viewer data plane) apply the SAME cap as
/// direct traffic rather than an inconsistent limit of their own.
pub fn max_request_body_bytes() -> usize {
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

/// Whether the request's `user-agent` (or `x-amz-user-agent`) carries the
/// `api/neptune` SDK-metadata token the `aws-sdk-neptune` client stamps in.
/// Used to disambiguate Neptune from RDS, which share the `rds` SigV4 scope
/// and endpoint. Matches the `api/neptune` token that AWS SDKs emit (a
/// leading `api/neptune` optionally followed by `#`/`/` and a version).
fn user_agent_indicates_neptune(headers: &http::HeaderMap) -> bool {
    for name in ["user-agent", "x-amz-user-agent"] {
        if let Some(ua) = headers.get(name).and_then(|v| v.to_str().ok()) {
            for part in ua.split_whitespace() {
                if let Some(rest) = part.strip_prefix("api/neptune") {
                    if rest.is_empty() || rest.starts_with('#') || rest.starts_with('/') {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Extract region from User-Agent header suffix `region/<region>`.
/// Whether the request's `user-agent` (or `x-amz-user-agent`) carries the
/// `api/docdb` SDK-metadata token the `aws-sdk-docdb` client stamps in. Used
/// to disambiguate DocumentDB from RDS, which share the `rds` SigV4 scope
/// and endpoint. Matches the `api/docdb` token that AWS SDKs emit (a leading
/// `api/docdb` optionally followed by `#`/`/` and a version).
fn user_agent_indicates_docdb(headers: &http::HeaderMap) -> bool {
    for name in ["user-agent", "x-amz-user-agent"] {
        if let Some(ua) = headers.get(name).and_then(|v| v.to_str().ok()) {
            for part in ua.split_whitespace() {
                if let Some(rest) = part.strip_prefix("api/docdb") {
                    if rest.is_empty() || rest.starts_with('#') || rest.starts_with('/') {
                        return true;
                    }
                }
            }
        }
    }
    false
}

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

/// The wire shape of an error body. It follows the protocol, except within
/// REST-XML: S3 answers with a bare `<Error>` document, while CloudFront and
/// Route 53 wrap theirs in a namespaced `<ErrorResponse>`. The AWS SDKs parse
/// each service's errors strictly against its own shape.
#[derive(Clone, Copy, Debug)]
struct ErrorEnvelope {
    protocol: AwsProtocol,
    /// `Some(xmlns)` selects the `<ErrorResponse>` REST-XML shape.
    rest_xml_namespace: Option<&'static str>,
    /// An S3 Control request (`s3-control` host, served by the `s3`
    /// handler): its errors use S3 Control's un-namespaced `<ErrorResponse>`.
    s3_control: bool,
    /// A REST-JSON service whose errors use Lambda's body shape.
    lambda_body: bool,
}

impl ErrorEnvelope {
    /// The envelope for an error answering a request routed to `detected`.
    fn for_request(detected: &protocol::DetectedRequest, headers: &http::HeaderMap) -> Self {
        let rest_xml_namespace = if detected.protocol == AwsProtocol::Rest {
            fakecloud_aws::error::rest_xml_error_namespace(&detected.service)
        } else {
            None
        };
        Self {
            protocol: detected.protocol,
            rest_xml_namespace,
            s3_control: detected.protocol == AwsProtocol::Rest
                && detected.service == "s3"
                && protocol::is_s3_control_host(headers),
            lambda_body: detected.protocol == AwsProtocol::RestJson
                && fakecloud_aws::error::uses_lambda_error_body(&detected.service),
        }
    }
}

impl From<AwsProtocol> for ErrorEnvelope {
    fn from(protocol: AwsProtocol) -> Self {
        Self {
            protocol,
            rest_xml_namespace: None,
            s3_control: false,
            lambda_body: false,
        }
    }
}

fn build_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    envelope: impl Into<ErrorEnvelope>,
) -> Response<Body> {
    build_error_response_with_fields(status, code, message, request_id, envelope, &[])
}

fn build_error_response_with_fields(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    envelope: impl Into<ErrorEnvelope>,
    extra_fields: &[(String, String)],
) -> Response<Body> {
    let envelope = envelope.into();
    let (status, content_type, body) = match (envelope.protocol, envelope.rest_xml_namespace) {
        // awsQuery services (SQS, SNS, IAM, STS, RDS, ELBv2, CloudWatch,
        // AutoScaling, ...) share the `<ErrorResponse>` envelope.
        (AwsProtocol::Query, _) => {
            fakecloud_aws::error::xml_error_response(status, code, message, request_id)
        }
        // EC2 uses the distinct ec2Query error envelope
        // (`<Response><Errors><Error>...</Errors><RequestID>`). Only EC2 is
        // classified `Ec2Query`, so the other Query-protocol services above are
        // untouched.
        (AwsProtocol::Ec2Query, _) => {
            fakecloud_aws::ec2query::ec2_error_response(status, code, message, request_id)
        }
        // CloudFront and Route 53 wrap the error in a namespaced
        // `<ErrorResponse>`; S3 (and the REST-XML fallbacks) use a bare `<Error>`.
        (AwsProtocol::Rest, Some(namespace)) => fakecloud_aws::error::rest_xml_error_response(
            status, code, message, request_id, namespace,
        ),
        (AwsProtocol::Rest, None) if envelope.s3_control => {
            fakecloud_aws::error::s3_control_xml_error_response(
                status,
                code,
                message,
                request_id,
                extra_fields,
            )
        }
        (AwsProtocol::Rest, None) => fakecloud_aws::error::s3_xml_error_response_with_fields(
            status,
            code,
            message,
            request_id,
            extra_fields,
        ),
        (AwsProtocol::RestJson, _) if envelope.lambda_body => {
            fakecloud_aws::error::lambda_json_error_response(status, code, message, extra_fields)
        }
        (AwsProtocol::Json | AwsProtocol::RestJson, _) => {
            fakecloud_aws::error::json_error_response_with_fields(
                status,
                code,
                message,
                extra_fields,
            )
        }
        (AwsProtocol::RpcV2Cbor, _) => (
            status,
            crate::cbor::CBOR_CONTENT_TYPE.to_string(),
            Bytes::from(crate::cbor::error_body(code, message, extra_fields)),
        ),
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
    // restJson1 services name the error in `x-amzn-ErrorType`, the header
    // the SDKs read first (Lambda's bodies carry no code at all).
    if envelope.protocol == AwsProtocol::RestJson {
        if let Ok(v) = http::HeaderValue::from_str(&safe_code) {
            builder = builder.header("x-amzn-errortype", v);
        }
    }
    if envelope.protocol == AwsProtocol::RpcV2Cbor {
        builder = builder.header(
            crate::cbor::SMITHY_PROTOCOL_HEADER,
            crate::cbor::RPC_V2_CBOR,
        );
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
/// For an unsigned request that no other detection rule claimed, return the
/// bucket name when the first path segment names an existing S3 bucket.
///
/// fakecloud serves every service from one endpoint, so an anonymous
/// path-style S3 request (`GET /bucket/key`, no SigV4) is indistinguishable
/// from an API Gateway execute-api call by headers alone. Bucket existence is
/// the disambiguator: if the segment is a real bucket, route to S3; otherwise
/// fall through to the apigateway catch-all. Uses the already-wired
/// `resource_policy_provider`, which resolves S3 bucket ownership from state
/// (`Some` => the bucket exists). Returns `None` when no provider is wired.
/// Recover the access key from a SigV2 presigned URL. AWS SigV2 presigning
/// puts the key in the `AWSAccessKeyId` query parameter alongside `Signature`
/// and `Expires`; all three must be present for the URL to be a SigV2 presign.
/// Returns None for SigV4 presigns (which use `X-Amz-Credential`) or unsigned
/// requests.
fn sigv2_presigned_access_key(query_params: &HashMap<String, String>) -> Option<String> {
    if query_params.contains_key("Signature") && query_params.contains_key("Expires") {
        query_params.get("AWSAccessKeyId").cloned()
    } else {
        None
    }
}

/// True when `s` is a 64-character lowercase-hex SHA-256 digest — the form a
/// signed `x-amz-content-sha256` payload hash takes. Distinguishes a real body
/// hash from the `UNSIGNED-PAYLOAD` / `STREAMING-*` markers (shorter and
/// containing non-hex characters), so only genuine hashes are re-bound to the
/// buffered body.
fn is_hex_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Lowercase-hex SHA-256 of `bytes`, used to re-bind a signed
/// `x-amz-content-sha256` to the buffered request body for non-S3 services.
fn sha256_hex_lower(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// The condition context for evaluating `iam_action` as `principal`: the
/// principal's global keys, the session keys its credential carries (MFA,
/// token issue time, federated provider), the service's own condition keys,
/// and the resource / request / principal tags for ABAC. Callers add the
/// keys that depend on the resource's owner (see [`add_global_request_keys`]).
#[allow(clippy::too_many_arguments)]
fn principal_condition_context(
    principal: &Principal,
    resolved: Option<&crate::auth::ResolvedCredential>,
    service: &dyn crate::service::AwsService,
    aws_request: &AwsRequest,
    iam_action: &crate::auth::IamAction,
    detected: &protocol::DetectedRequest,
    remote_addr: Option<SocketAddr>,
) -> ConditionContext {
    let mut ctx = build_condition_context(
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
    if let Some(rc) = resolved {
        // An assumed-role session's `aws:PrincipalArn` is its role's ARN
        // (path included), not the session ARN.
        if let Some(role_arn) = rc.role_arn.as_ref() {
            ctx.aws_principal_arn = Some(role_arn.clone());
        }
        ctx.aws_mfa_present = Some(rc.mfa_present);
        ctx.aws_token_issue_time = rc.token_issued_at;
        ctx.aws_federated_provider = rc.federated_provider.clone();
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
                ctx.aws_mfa_age_seconds = Some(age);
            }
        }
    }
    ctx.service_keys = service.iam_condition_keys_for(aws_request, iam_action);

    // ABAC: populate tag-based condition keys.
    // aws:ResourceTag/*. An `iam:PassRole` resource is
    // an IAM role, not one of this service's resources,
    // so the service has no tags (or resource policy)
    // for it.
    let service_resource = !iam_action.is_pass_role();
    match service_resource
        .then(|| service.resource_tags_for(&iam_action.resource))
        .flatten()
    {
        Some(tags) => ctx.resource_tags = Some(tags),
        None => tracing::debug!(
            target: "fakecloud::iam::audit",
            service = %detected.service,
            resource = %iam_action.resource,
            "service does not expose resource tags for ABAC; skipping aws:ResourceTag/* evaluation"
        ),
    }
    // aws:RequestTag/* + aws:TagKeys
    match service.request_tags_from(aws_request, iam_action.action) {
        Some(tags) => ctx.request_tags = Some(tags),
        None => tracing::debug!(
            target: "fakecloud::iam::audit",
            service = %detected.service,
            action = %iam_action.action_string(),
            "service does not expose request tags for ABAC; skipping aws:RequestTag/* / aws:TagKeys evaluation"
        ),
    }
    // aws:PrincipalTag/*
    ctx.principal_tags = principal.tags.clone();
    ctx
}

/// Populate `aws:ResourceAccount` (the account owning the target resource)
/// and, when the principal's account is in an organization,
/// `aws:PrincipalOrgID` / `aws:PrincipalOrgPaths`. A value the service
/// already supplied wins.
fn add_global_request_keys(
    ctx: &mut ConditionContext,
    principal: &Principal,
    resource_account_id: &str,
    scp_resolver: Option<&dyn crate::auth::ScpResolver>,
) {
    if !resource_account_id.is_empty() {
        ctx.service_keys
            .entry("aws:resourceaccount".to_string())
            .or_insert_with(|| vec![resource_account_id.to_string()]);
    }
    if let Some((org_id, path)) = scp_resolver.and_then(|r| r.principal_org(&principal.account_id))
    {
        ctx.service_keys
            .entry("aws:principalorgid".to_string())
            .or_insert_with(|| vec![org_id]);
        ctx.service_keys
            .entry("aws:principalorgpaths".to_string())
            .or_insert_with(|| vec![path]);
    }
}

/// The AWS error for a request whose access key resolves to no identity:
/// `ExpiredToken` for a temporary credential that has expired, otherwise
/// `InvalidClientTokenId` (unknown or deactivated key, revoked session).
fn unresolved_credential_response(
    config: &DispatchConfig,
    access_key_id: &str,
    request_id: &str,
    envelope: ErrorEnvelope,
) -> Response<Body> {
    let expired = config
        .credential_resolver
        .as_ref()
        .is_some_and(|r| r.is_expired(access_key_id));
    let (code, message) = if expired {
        (
            "ExpiredToken",
            "The security token included in the request is expired",
        )
    } else {
        invalid_token_error(&envelope)
    };
    build_error_response(StatusCode::FORBIDDEN, code, message, request_id, envelope)
}

/// The code and message for an access key that resolves to no identity.
/// Lambda answers `UnrecognizedClientException` with a trailing period
/// (recorded in us-east-1); the other services keep `InvalidClientTokenId`.
fn invalid_token_error(envelope: &ErrorEnvelope) -> (&'static str, &'static str) {
    if envelope.lambda_body {
        (
            "UnrecognizedClientException",
            "The security token included in the request is invalid.",
        )
    } else {
        (
            "InvalidClientTokenId",
            "The security token included in the request is invalid",
        )
    }
}

/// Whether a request no protocol signal routed is an unsigned S3 path-style
/// request. It must be fully unsigned (no `Authorization`, no SigV4 or SigV2
/// presign parameters: a signed request whose scope named no REST service is
/// an IAM-auth API invocation signed for `execute-api`), its path must not be
/// a fakecloud endpoint (`/_*`; no bucket name starts with `_`), and the API
/// Gateway data plane must not claim it (execute-api or custom-domain host,
/// path-style invocation URL, or a deployed stage as the first segment; see
/// [`crate::service::AwsService::claims_unsigned_request`]).
fn unsigned_request_is_s3_path_style(
    method: &http::Method,
    uri: &http::Uri,
    headers: &http::HeaderMap,
    query_params: &HashMap<String, String>,
    registry: &ServiceRegistry,
    config: &DispatchConfig,
) -> bool {
    if !is_unsigned(headers, query_params)
        || uri.path().starts_with("/_")
        || registry.get("s3").is_none()
    {
        return false;
    }
    !registry.get("apigateway").is_some_and(|apigw| {
        apigw.claims_unsigned_request(method, uri.path(), headers, &config.account_id)
    })
}

fn anonymous_s3_bucket(uri: &http::Uri, config: &DispatchConfig) -> Option<String> {
    let provider = config.resource_policy_provider.as_ref()?;
    let segment = uri.path().split('/').find(|s| !s.is_empty())?.to_string();
    let arn = fakecloud_aws::arn::Arn::s3(&segment).to_string();
    provider.resource_owner_account("s3", &arn).map(|_| segment)
}

/// The account an [`InternalCaller`] request works in: for S3, the owner of
/// the addressed bucket (bucket names are global, and a bucket policy can
/// grant a service acting for another account's resource); otherwise, or
/// when the bucket does not exist, the account the caller acts for.
fn internal_caller_account(
    caller: &InternalCaller,
    service: &str,
    path_segments: &[String],
    config: &DispatchConfig,
) -> String {
    let bucket_owner = (service == "s3")
        .then(|| path_segments.first())
        .flatten()
        .and_then(|bucket| {
            let arn = fakecloud_aws::arn::Arn::s3(bucket).to_string();
            config
                .resource_policy_provider
                .as_ref()?
                .resource_owner_account("s3", &arn)
        });
    bucket_owner.unwrap_or_else(|| caller.acting_account().to_string())
}

/// Authorize a request fakecloud makes in-process as an AWS-owned principal
/// (see [`InternalCaller`]). Such a principal has no identity policies and no
/// account, so -- as in AWS -- only the resource policy (S3 bucket policy) can
/// grant it the action, with a public-read ACL honored the way it is for any
/// caller. The request context carries the caller's keys (`aws:SourceArn`,
/// `aws:SourceAccount`, ...) so a confused-deputy condition scoped to the
/// acting resource matches. `aws:SourceIp` is absent, as AWS omits it for a
/// request a service makes.
///
/// Returns the error response to send when the request is denied under
/// strict mode; soft mode only logs.
fn authorize_internal_caller(
    caller: &InternalCaller,
    service: &dyn crate::service::AwsService,
    aws_request: &AwsRequest,
    evaluator: &dyn IamPolicyEvaluator,
    config: &DispatchConfig,
    detected: &protocol::DetectedRequest,
    request_id: &str,
) -> Option<Response<Body>> {
    let principal = caller.principal();
    let denied = || {
        config.iam_mode.is_strict().then(|| {
            build_error_response(
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "Access Denied",
                request_id,
                ErrorEnvelope::for_request(detected, &aws_request.headers),
            )
        })
    };
    let iam_actions = service.iam_actions_for(aws_request);
    if iam_actions.is_empty() {
        tracing::warn!(
            target: "fakecloud::iam::audit",
            service = %detected.service,
            action = %aws_request.action,
            principal = %principal.arn,
            mode = %config.iam_mode,
            request_id = %request_id,
            "service-principal request has no IamAction mapping; denying under strict, allowing under soft"
        );
        return denied();
    }
    // A service acting on AWS's behalf is not subject to `iam:PassRole`:
    // that check binds the customer principal handing a role to a service.
    for iam_action in iam_actions.iter().filter(|a| !a.is_pass_role()) {
        let now = chrono::Utc::now();
        let mut context = ConditionContext {
            aws_principal_arn: Some(principal.arn.clone()),
            aws_current_time: Some(now),
            aws_epoch_time: Some(now.timestamp()),
            aws_secure_transport: Some(is_secure_transport(&aws_request.headers)),
            aws_requested_region: Some(aws_request.region.clone()),
            ..Default::default()
        };
        context.service_keys = service.iam_condition_keys_for(aws_request, iam_action);
        context.service_keys.extend(caller.condition_keys());
        // `aws:ResourceAccount`: the account the service works in for this
        // request (for S3, the addressed bucket's owner).
        context
            .service_keys
            .entry("aws:resourceaccount".to_string())
            .or_insert_with(|| vec![aws_request.account_id.clone()]);
        let resource_policy_json = config
            .resource_policy_provider
            .as_ref()
            .and_then(|p| p.resource_policy(&detected.service, &iam_action.resource));
        let decision = evaluator.evaluate_resource_policy_only(
            &principal,
            iam_action,
            &context,
            resource_policy_json.as_deref(),
        );
        let explicit_deny = matches!(decision, crate::auth::IamDecision::ExplicitDeny);
        let acl_allows = !explicit_deny
            && config.resource_policy_provider.as_ref().is_some_and(|p| {
                p.public_acl_allows(&detected.service, &iam_action.resource, iam_action.action)
            });
        if !decision.is_allow() && !acl_allows {
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
                "service-principal request denied: the resource policy does not grant the action"
            );
            if let Some(resp) = denied() {
                return Some(resp);
            }
        }
    }
    None
}

/// Whether a grant on `action.resource` authorizes a request its policies
/// left implicitly denied (see [`ResourcePolicyProvider::grant_authorization`]).
/// A grant naming the caller itself authorizes it without an identity
/// policy, in the key's account or another, but never past the caller's SCP
/// ceiling; a grant naming the role of an assumed-role session is also
/// capped by the role's permissions boundary and the session's session
/// policies (a grant naming the user or session itself is not). One naming
/// the caller's account
/// only lets the account delegate, so the caller's identity policies (with
/// their boundary, session and SCP gates) must also allow the action.
#[allow(clippy::too_many_arguments)]
fn grant_authorizes(
    provider: Option<&dyn ResourcePolicyProvider>,
    evaluator: &dyn IamPolicyEvaluator,
    service: &str,
    request: &AwsRequest,
    action: &crate::auth::IamAction,
    principal: &Principal,
    context: &ConditionContext,
    session_policies: &[String],
    scps: Option<&[String]>,
) -> bool {
    let Some(provider) = provider else {
        return false;
    };
    match provider.grant_authorization(service, request, action, principal) {
        Some(crate::auth::GrantAuthorization::Grantee) => evaluator
            .evaluate_ceiling(principal, action, context, &[], scps, false)
            .is_allow(),
        Some(crate::auth::GrantAuthorization::GranteeRole) => evaluator
            .evaluate_ceiling(principal, action, context, session_policies, scps, true)
            .is_allow(),
        Some(crate::auth::GrantAuthorization::Account) => evaluator
            .evaluate(principal, action, context, session_policies, scps)
            .is_allow(),
        None => false,
    }
}

/// Authorize a request made as an account's root user: its SCPs first, then,
/// for a resource of another account, that resource's policy (see
/// [`authorize_root_cross_account`]).
///
/// Returns the error response to send when the request is denied under
/// strict mode; soft mode only logs.
#[allow(clippy::too_many_arguments)]
fn authorize_root(
    principal: &Principal,
    service: &dyn crate::service::AwsService,
    aws_request: &AwsRequest,
    evaluator: &dyn IamPolicyEvaluator,
    config: &DispatchConfig,
    detected: &protocol::DetectedRequest,
    request_id: &str,
    remote_addr: Option<SocketAddr>,
    resolved: Option<&crate::auth::ResolvedCredential>,
) -> Option<Response<Body>> {
    authorize_root_scps(
        principal,
        service,
        aws_request,
        evaluator,
        config,
        detected,
        request_id,
        remote_addr,
        resolved,
    )
    .or_else(|| {
        authorize_root_cross_account(
            principal,
            service,
            aws_request,
            evaluator,
            config,
            detected,
            request_id,
            remote_addr,
            resolved,
        )
    })
}

/// Cap a root-user request at the account's SCPs.
///
/// Root needs no identity policy in its own account, but SCPs restrict
/// "IAM users and roles in member accounts, including the member account's
/// root user" (Organizations user guide, "Service control policies"), and
/// AWS evaluates them before any resource-based policy, so a resource policy
/// granting root in another account does not reach past them either. The
/// resolver exempts the management account (and returns nothing outside an
/// organization), which keeps root there unrestricted.
#[allow(clippy::too_many_arguments)]
fn authorize_root_scps(
    principal: &Principal,
    service: &dyn crate::service::AwsService,
    aws_request: &AwsRequest,
    evaluator: &dyn IamPolicyEvaluator,
    config: &DispatchConfig,
    detected: &protocol::DetectedRequest,
    request_id: &str,
    remote_addr: Option<SocketAddr>,
    resolved: Option<&crate::auth::ResolvedCredential>,
) -> Option<Response<Body>> {
    let resolver = config.scp_resolver.as_deref()?;
    let scps = resolver.scps_for(principal)?;
    let in_caller_account = service.iam_resource_in_caller_account(aws_request);
    let iam_actions = service.iam_actions_for(aws_request);
    if iam_actions.is_empty() {
        // An operation with no IAM action mapping cannot be checked against
        // the SCPs, so under strict enforcement it fails closed, as it does
        // for every other principal.
        tracing::warn!(
            target: "fakecloud::iam::audit",
            service = %detected.service,
            action = %aws_request.action,
            principal = %principal.arn,
            mode = %config.iam_mode,
            request_id = %request_id,
            "root request under SCPs has no IamAction mapping; denying under strict, allowing under soft"
        );
        return config
            .iam_mode
            .is_strict()
            .then(|| unmapped_action_denial(principal, aws_request, request_id, detected));
    }
    for iam_action in iam_actions {
        let resource_account = config
            .resource_policy_provider
            .as_ref()
            .filter(|_| !iam_action.is_pass_role() && !in_caller_account)
            .and_then(|p| p.resource_owner_account(&detected.service, &iam_action.resource))
            .or_else(|| parse_account_from_arn(&iam_action.resource))
            .unwrap_or_else(|| principal.account_id.clone());
        let mut context = principal_condition_context(
            principal,
            resolved,
            service,
            aws_request,
            &iam_action,
            detected,
            remote_addr,
        );
        add_global_request_keys(&mut context, principal, &resource_account, Some(resolver));
        let decision =
            evaluator.evaluate_ceiling(principal, &iam_action, &context, &[], Some(&scps), false);
        if decision.is_allow() {
            continue;
        }
        tracing::warn!(
            target: "fakecloud::iam::audit",
            service = %detected.service,
            action = %iam_action.action_string(),
            resource = %iam_action.resource,
            principal = %principal.arn,
            decision = ?decision,
            mode = %config.iam_mode,
            request_id = %request_id,
            "root request denied by the account's service control policies"
        );
        if config.iam_mode.is_strict() {
            let action = iam_action.action_string();
            return Some(access_denied_response(
                &not_authorized_message(
                    &principal.arn,
                    &action,
                    &iam_action.resource,
                    scp_denial_reason(decision, &action).as_deref(),
                    None,
                ),
                request_id,
                detected,
                &aws_request.headers,
            ));
        }
    }
    None
}

/// AWS's wording for a request the SCPs refuse, given the SCP ceiling's
/// `decision`: `with an explicit deny in a service control policy`, or
/// `because no service control policy allows the <action> action`. `None`
/// when the SCPs allow the request.
fn scp_denial_reason(decision: crate::auth::IamDecision, action: &str) -> Option<String> {
    match decision {
        crate::auth::IamDecision::Allow => None,
        crate::auth::IamDecision::ExplicitDeny => {
            Some("with an explicit deny in a service control policy".to_string())
        }
        crate::auth::IamDecision::ImplicitDeny => Some(format!(
            "because no service control policy allows the {action} action"
        )),
    }
}

/// The SCP ceiling's decision when the SCP layer itself caused a denial
/// whose overall decision was `decision`: an SCP that explicitly denies, or
/// one that doesn't allow when nothing else explicitly denied (an explicit
/// Deny elsewhere is the cause then). `None` when the SCPs are not the
/// cause.
fn scp_cause(
    decision: crate::auth::IamDecision,
    ceiling: crate::auth::IamDecision,
) -> Option<crate::auth::IamDecision> {
    use crate::auth::IamDecision;
    match (decision, ceiling) {
        (_, IamDecision::ExplicitDeny) => Some(IamDecision::ExplicitDeny),
        (IamDecision::ImplicitDeny, IamDecision::ImplicitDeny) => Some(IamDecision::ImplicitDeny),
        _ => None,
    }
}

/// The message of an `AccessDeniedException` for `principal_arn`:
/// `User: <arn> is not authorized to perform: <action> on resource:
/// <resource>`, then the `reason` the request was refused when one is known
/// (an SCP's), then the encoded authorization failure message if any.
fn not_authorized_message(
    principal_arn: &str,
    action: &str,
    resource: &str,
    reason: Option<&str>,
    encoded: Option<&str>,
) -> String {
    let mut message = format!(
        "User: {principal_arn} is not authorized to perform: {action} on resource: {resource}"
    );
    if let Some(reason) = reason {
        message.push(' ');
        message.push_str(reason);
    }
    if let Some(encoded) = encoded {
        message.push_str(" Encoded authorization failure message: ");
        message.push_str(encoded);
    }
    message
}

/// The error code a caller its policies don't authorize is answered with,
/// by the request's protocol: `AccessDenied` on the awsQuery protocol (IAM,
/// STS, SQS's query path, ELBv2, ...) and for S3, `AccessDeniedException` on
/// the JSON and REST protocols (their declared code), except SNS, whose
/// declared 403 is `AuthorizationError`.
fn access_denied_code(service: &str, protocol: AwsProtocol) -> &'static str {
    match (service, protocol) {
        ("sns", _) => "AuthorizationError",
        ("s3", _) | (_, AwsProtocol::Query) => "AccessDenied",
        _ => "AccessDeniedException",
    }
}

/// Services that answer the JSON protocol `awsQueryCompatible`: their JSON
/// errors carry the awsQuery code in `x-amzn-query-error`, which SDKs report
/// as the error's code.
const QUERY_COMPATIBLE_JSON_SERVICES: &[&str] = &["sqs"];

/// The strict-mode refusal of a request its caller is not authorized for,
/// shaped as the service answers it (see [`access_denied_code`]). On the
/// JSON path of an awsQuery-compatible service (SQS), the body keeps the
/// JSON `AccessDeniedException` and `x-amzn-query-error: AccessDenied;Sender`
/// carries the awsQuery code, as SQS does for its own errors.
fn access_denied_response(
    message: &str,
    request_id: &str,
    detected: &protocol::DetectedRequest,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let mut resp = build_error_response(
        StatusCode::FORBIDDEN,
        access_denied_code(&detected.service, detected.protocol),
        message,
        request_id,
        ErrorEnvelope::for_request(detected, headers),
    );
    if detected.protocol == AwsProtocol::Json
        && QUERY_COMPATIBLE_JSON_SERVICES.contains(&detected.service.as_str())
    {
        resp.headers_mut().insert(
            "x-amzn-query-error",
            http::HeaderValue::from_static("AccessDenied;Sender"),
        );
    }
    resp
}

/// The strict-mode refusal of an operation with no IAM action mapping,
/// which cannot be authorized.
fn unmapped_action_denial(
    principal: &Principal,
    aws_request: &AwsRequest,
    request_id: &str,
    detected: &protocol::DetectedRequest,
) -> Response<Body> {
    access_denied_response(
        &format!(
            "User: {} is not authorized to perform: {}: no IAM action mapping exists for this operation, so it cannot be authorized under strict IAM enforcement",
            principal.arn, aws_request.action,
        ),
        request_id,
        detected,
        &aws_request.headers,
    )
}

/// Authorize an account root reaching a resource ANOTHER account owns.
///
/// Root needs no identity policy in its own account, which is why dispatch
/// exempts it from identity evaluation. That exemption ends at the account
/// boundary: on AWS, root of account B reaching account A's bucket (or table)
/// is a cross-account request like any other, allowed only when A's resource
/// policy grants it. This applies where the resource policy provider resolves
/// the owner of a resource (S3 buckets, whose names are global and whose ARNs
/// carry no account, and DynamoDB tables and streams) -- the resource-policy
/// governed data services whose handlers serve a request in the owner's
/// account. A same-account resource, or one no provider claims, keeps the
/// root exemption.
///
/// Returns the error response to send when the request is denied under
/// strict mode; soft mode only logs.
#[allow(clippy::too_many_arguments)]
fn authorize_root_cross_account(
    principal: &Principal,
    service: &dyn crate::service::AwsService,
    aws_request: &AwsRequest,
    evaluator: &dyn IamPolicyEvaluator,
    config: &DispatchConfig,
    detected: &protocol::DetectedRequest,
    request_id: &str,
    remote_addr: Option<SocketAddr>,
    resolved: Option<&crate::auth::ResolvedCredential>,
) -> Option<Response<Body>> {
    if service.iam_resource_in_caller_account(aws_request) {
        return None;
    }
    let provider = config.resource_policy_provider.as_ref()?;
    for iam_action in service.iam_actions_for(aws_request) {
        let Some(owner) = provider.resource_owner_account(&detected.service, &iam_action.resource)
        else {
            continue;
        };
        if owner == principal.account_id {
            continue;
        }
        let mut context = principal_condition_context(
            principal,
            resolved,
            service,
            aws_request,
            &iam_action,
            detected,
            remote_addr,
        );
        add_global_request_keys(
            &mut context,
            principal,
            &owner,
            config.scp_resolver.as_deref(),
        );
        let resource_policy_json = provider
            .resource_policy_applies(&detected.service, &iam_action)
            .then(|| provider.resource_policy(&detected.service, &iam_action.resource))
            .flatten();
        let decision = evaluator.evaluate_resource_policy_only(
            principal,
            &iam_action,
            &context,
            resource_policy_json.as_deref(),
        );
        let explicit_deny = matches!(decision, crate::auth::IamDecision::ExplicitDeny);
        let acl_allows = !explicit_deny
            && provider.public_acl_allows(
                &detected.service,
                &iam_action.resource,
                iam_action.action,
            );
        // A grant naming the root or its account authorizes it: the root
        // holds every permission of its own account, so a grant to the
        // account needs no further IAM delegation.
        let grant_allows = decision == crate::auth::IamDecision::ImplicitDeny
            && provider
                .grant_authorization(&detected.service, aws_request, &iam_action, principal)
                .is_some();
        if decision.is_allow() || acl_allows || grant_allows {
            continue;
        }
        tracing::warn!(
            target: "fakecloud::iam::audit",
            service = %detected.service,
            action = %iam_action.action_string(),
            resource = %iam_action.resource,
            principal = %principal.arn,
            resource_account = %owner,
            resource_policy_present = resource_policy_json.is_some(),
            decision = ?decision,
            mode = %config.iam_mode,
            request_id = %request_id,
            "cross-account root request denied: the resource policy does not grant the action"
        );
        if config.iam_mode.is_strict() {
            return Some(access_denied_response(
                &format!(
                    "User: {} is not authorized to perform: {} on resource: {} because no resource-based policy allows the {} action",
                    principal.arn,
                    iam_action.action_string(),
                    iam_action.resource,
                    iam_action.action_string(),
                ),
                request_id,
                detected,
                &aws_request.headers,
            ));
        }
    }
    None
}

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
pub fn principal_type_label(t: PrincipalType) -> &'static str {
    match t {
        PrincipalType::User => "User",
        PrincipalType::AssumedRole => "AssumedRole",
        PrincipalType::FederatedUser => "FederatedUser",
        PrincipalType::Root => "Account",
        PrincipalType::Unknown => "Unknown",
        PrincipalType::Service => "Service",
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

/// Whether a (possibly percent-encoded) `/tags/{resourceArn}` label is the ARN
/// of a Bedrock Agents runtime session (`...:session/<id>`).
fn names_bedrock_session(label: &str) -> bool {
    let decoded = label
        .to_ascii_lowercase()
        .replace("%3a", ":")
        .replace("%2f", "/");
    decoded.starts_with("arn:") && decoded.contains(":bedrock:") && decoded.contains(":session/")
}

/// Which Bedrock Agents handler a `bedrock`-scoped request belongs to:
/// `bedrock-agent-runtime` or `bedrock-agent` for their path families, `None`
/// for any other Bedrock (bedrock-runtime / control plane) path. Runtime and
/// control plane share `/agents/...` and `/flows/...` prefixes, so the split
/// is by path shape and, where the shapes coincide, by method.
fn bedrock_agent_service_for(method: &http::Method, path: &str) -> Option<&'static str> {
    let first_seg = path.split('/').nth(1);
    if !matches!(
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
        return None;
    }
    let segs: Vec<&str> = path.split('/').collect();
    let is_runtime = matches!(
        segs.as_slice(),
        ["", "agents", _, "agentAliases", _, ..]  // InvokeAgent
            | ["", "flows", _, "executions"] // ListFlowExecutions
            | ["", "flows", _, "aliases", _, "executions", ..] // flow executions
            | ["", "knowledgebases", _, "retrieve"] // Retrieve
            | ["", "retrieveAndGenerate"]
            | ["", "retrieveAndGenerateStream"]
            | ["", "optimize-prompt"]
            | ["", "sessions", ..]
            | ["", "invocations", ..]
            | ["", "generate-query"]
            | ["", "rerank"]
    ) || (
        // InvokeFlow is POST /flows/{flow}/aliases/{alias}; the same path
        // under GET / PUT / DELETE is the control plane's flow-alias CRUD.
        *method == http::Method::POST && matches!(segs.as_slice(), ["", "flows", _, "aliases", _])
    ) || (
        // InvokeInlineAgent is POST /agents/{sessionId}; the control plane's
        // PrepareAgent is POST /agents/{agentId}/ (trailing slash).
        *method == http::Method::POST
            && matches!(segs.as_slice(), ["", "agents", id] if !id.is_empty())
    ) || path
        // Tagging is shared: a runtime resource (a session) is tagged through
        // the runtime, an agent/flow/knowledge-base resource through the
        // control plane.
        .strip_prefix("/tags/")
        .is_some_and(names_bedrock_session);
    Some(if is_runtime {
        "bedrock-agent-runtime"
    } else {
        "bedrock-agent"
    })
}

#[cfg(test)]
mod tests {

    fn gzip(data: &[u8]) -> Bytes {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(data).unwrap();
        Bytes::from(enc.finish().unwrap())
    }

    fn gzip_headers(extra: &[(&'static str, &str)]) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        h.insert("content-encoding", "gzip".parse().unwrap());
        for (k, v) in extra {
            h.insert(*k, v.parse().unwrap());
        }
        h
    }

    #[test]
    fn request_compression_decodes_gzip_for_cloudwatch() {
        let body = br#"{"Namespace":"App"}"#;
        // awsJson (X-Amz-Target).
        let h = gzip_headers(&[(
            "x-amz-target",
            "GraniteServiceVersion20100801.PutMetricData",
        )]);
        assert_eq!(
            decode_request_compression(&h, None, gzip(body)).unwrap(),
            Bytes::from_static(body)
        );
        // awsQuery (SigV4 scope only).
        let h = gzip_headers(&[(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=0",
        )]);
        let form = b"Action=PutMetricData&Namespace=App";
        assert_eq!(
            decode_request_compression(&h, None, gzip(form)).unwrap(),
            Bytes::from_static(form)
        );
        // rpcv2Cbor.
        let detected = protocol::DetectedRequest {
            service: "monitoring".to_string(),
            action: "PutMetricData".to_string(),
            protocol: AwsProtocol::RpcV2Cbor,
        };
        let h = gzip_headers(&[]);
        assert_eq!(
            decode_request_compression(&h, Some(&detected), gzip(&[0xa0])).unwrap(),
            Bytes::from_static(&[0xa0])
        );
        // Corrupt gzip is a serialization error in the caller's protocol.
        let err = decode_request_compression(&h, Some(&detected), Bytes::from_static(b"nope"))
            .unwrap_err();
        assert_eq!(err.1, AwsProtocol::RpcV2Cbor);
    }

    #[test]
    fn request_compression_leaves_other_services_alone() {
        // S3 keeps Content-Encoding as object metadata: the body is stored as sent.
        let h = gzip_headers(&[(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0",
        )]);
        let body = gzip(b"object bytes");
        assert_eq!(
            decode_request_compression(&h, None, body.clone()).unwrap(),
            body
        );
        // No Content-Encoding: untouched.
        let h = http::HeaderMap::new();
        assert_eq!(
            decode_request_compression(&h, None, Bytes::from_static(b"x")).unwrap(),
            Bytes::from_static(b"x")
        );
    }

    #[test]
    fn request_compression_services_match_the_models() {
        // Every vendored model with an `@requestCompression` operation must be
        // listed (by registry name) in REQUEST_COMPRESSION_SERVICES.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../aws-models");
        let mut with_trait: Vec<String> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .filter_map(|e| e.ok())
            .filter(|e| {
                std::fs::read_to_string(e.path())
                    .is_ok_and(|s| s.contains("\"smithy.api#requestCompression\""))
            })
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        with_trait.sort();
        assert_eq!(
            with_trait,
            vec!["cloudwatch.json".to_string()],
            "update REQUEST_COMPRESSION_SERVICES for new @requestCompression models"
        );
        assert_eq!(REQUEST_COMPRESSION_SERVICES, &["monitoring"]);
    }
    #[test]
    fn bedrock_agent_paths_split_between_runtime_and_control_plane() {
        use http::Method;
        let runtime = Some("bedrock-agent-runtime");
        let agent = Some("bedrock-agent");
        for (method, path, want) in [
            (Method::POST, "/flows/F/aliases/A", runtime),
            (Method::GET, "/flows/F/aliases/A", agent),
            (Method::PUT, "/flows/F/aliases/A", agent),
            (Method::DELETE, "/flows/F/aliases/A", agent),
            (Method::POST, "/flows/F/aliases/A/executions", runtime),
            (Method::GET, "/flows/F/aliases/A/executions/E", runtime),
            (
                Method::POST,
                "/flows/F/aliases/A/executions/E/stop",
                runtime,
            ),
            (
                Method::GET,
                "/flows/F/aliases/A/executions/E/events",
                runtime,
            ),
            (
                Method::GET,
                "/flows/F/aliases/A/executions/E/flowsnapshot",
                runtime,
            ),
            (Method::GET, "/flows/F/executions", runtime),
            (Method::GET, "/flows/F/aliases", agent),
            (Method::POST, "/flows/F/versions", agent),
            (Method::GET, "/flows/F", agent),
            (
                Method::POST,
                "/agents/X/agentAliases/Y/sessions/S/text",
                runtime,
            ),
            (Method::GET, "/agents/X", agent),
            (Method::POST, "/agents/session-1", runtime),
            (Method::POST, "/agents/X/", agent),
            (Method::GET, "/agents/X/", agent),
            (Method::PUT, "/agents/X/", agent),
            (
                Method::POST,
                "/tags/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Asession%2F0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
                runtime,
            ),
            (
                Method::GET,
                "/tags/arn:aws:bedrock:us-east-1:123456789012:session/0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
                runtime,
            ),
            (
                Method::DELETE,
                "/tags/arn%3aaws%3abedrock%3aus-east-1%3a123456789012%3asession%2fabc",
                runtime,
            ),
            (
                Method::POST,
                "/tags/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Aagent%2FAGENT12345",
                agent,
            ),
            (
                Method::GET,
                "/tags/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Aflow%2FFLOW123456",
                agent,
            ),
            (Method::POST, "/model/m/invoke", None),
        ] {
            assert_eq!(
                bedrock_agent_service_for(&method, path),
                want,
                "{method} {path}"
            );
        }
    }

    use super::*;

    #[test]
    fn default_max_request_body_bytes_is_one_gib() {
        // Without the env override, the cap defaults to 1 GiB. The
        // public function caches via OnceLock so only the first call
        // in the process matters; we assert the constant directly.
        assert_eq!(DEFAULT_MAX_REQUEST_BODY_BYTES, 1024 * 1024 * 1024);
    }

    #[test]
    fn sigv2_presigned_access_key_extracted_with_signature_and_expires() {
        let mut q = HashMap::new();
        q.insert("AWSAccessKeyId".to_string(), "AKIAEXAMPLE".to_string());
        q.insert("Signature".to_string(), "abc%2Bdef".to_string());
        q.insert("Expires".to_string(), "1700000000".to_string());
        assert_eq!(
            sigv2_presigned_access_key(&q).as_deref(),
            Some("AKIAEXAMPLE")
        );
    }

    #[test]
    fn sigv2_presigned_access_key_none_without_signature_or_expires() {
        // AWSAccessKeyId alone (e.g. a stray query param) is not a SigV2
        // presign and must not be treated as a credential.
        let mut q = HashMap::new();
        q.insert("AWSAccessKeyId".to_string(), "AKIAEXAMPLE".to_string());
        assert_eq!(sigv2_presigned_access_key(&q), None);

        q.insert("Expires".to_string(), "1700000000".to_string());
        assert_eq!(
            sigv2_presigned_access_key(&q),
            None,
            "missing Signature must not qualify"
        );
    }

    #[test]
    fn sigv2_presigned_access_key_none_for_unsigned_request() {
        assert_eq!(sigv2_presigned_access_key(&HashMap::new()), None);
    }

    #[test]
    fn is_hex_sha256_accepts_real_digest_rejects_markers() {
        // A genuine 64-char lowercase-hex digest is a bindable body hash.
        assert!(is_hex_sha256(&sha256_hex_lower(b"hello")));
        assert!(is_hex_sha256(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        // The SigV4 payload markers are NOT hashes and must be skipped.
        assert!(!is_hex_sha256("UNSIGNED-PAYLOAD"));
        assert!(!is_hex_sha256("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));
        assert!(!is_hex_sha256("STREAMING-UNSIGNED-PAYLOAD-TRAILER"));
        // Wrong length or non-lowercase-hex characters are rejected.
        assert!(!is_hex_sha256("abc123"));
        assert!(!is_hex_sha256(
            "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
        ));
    }

    #[test]
    fn sha256_hex_lower_matches_known_vectors() {
        // Empty input -> the well-known SHA-256 of "".
        assert_eq!(
            sha256_hex_lower(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex_lower(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(sha256_hex_lower(b"abc").len(), 64);
    }

    #[test]
    fn global_request_keys_cover_resource_account_and_org() {
        struct Org;
        impl crate::auth::ScpResolver for Org {
            fn scps_for(&self, _: &Principal) -> Option<Vec<String>> {
                None
            }
            fn principal_org(&self, account: &str) -> Option<(String, String)> {
                (account == "111111111111")
                    .then(|| ("o-abc".to_string(), "o-abc/r-1/ou-2/".to_string()))
            }
        }
        let principal = |account: &str| Principal {
            arn: format!("arn:aws:iam::{account}:user/u"),
            user_id: "AIDAU".into(),
            account_id: account.into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let mut ctx = ConditionContext::default();
        add_global_request_keys(
            &mut ctx,
            &principal("111111111111"),
            "222222222222",
            Some(&Org),
        );
        assert_eq!(
            ctx.lookup("aws:ResourceAccount"),
            Some(vec!["222222222222".into()])
        );
        assert_eq!(ctx.lookup("aws:PrincipalOrgID"), Some(vec!["o-abc".into()]));
        assert_eq!(
            ctx.lookup("aws:PrincipalOrgPaths"),
            Some(vec!["o-abc/r-1/ou-2/".into()])
        );
        // A principal in no organization gets no org keys (AWS omits them).
        let mut ctx = ConditionContext::default();
        add_global_request_keys(
            &mut ctx,
            &principal("333333333333"),
            "333333333333",
            Some(&Org),
        );
        assert_eq!(ctx.lookup("aws:PrincipalOrgID"), None);
        assert_eq!(
            ctx.lookup("aws:ResourceAccount"),
            Some(vec!["333333333333".into()])
        );
    }

    #[test]
    fn unresolved_credential_distinguishes_expired_from_invalid() {
        struct Expired;
        impl CredentialResolver for Expired {
            fn resolve(&self, _: &str) -> Option<crate::auth::ResolvedCredential> {
                None
            }
            fn is_expired(&self, akid: &str) -> bool {
                akid == "ASIAEXPIRED"
            }
        }
        let mut cfg = DispatchConfig::new("us-east-1", "123456789012");
        cfg.credential_resolver = Some(Arc::new(Expired));
        let detected = protocol::DetectedRequest {
            service: "sts".into(),
            action: "GetCallerIdentity".into(),
            protocol: AwsProtocol::Query,
        };
        let code = |akid: &str| {
            let resp = unresolved_credential_response(
                &cfg,
                akid,
                "r",
                ErrorEnvelope::for_request(&detected, &http::HeaderMap::new()),
            );
            assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            resp.headers()["x-amz-error-code"]
                .to_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(code("ASIAEXPIRED"), "ExpiredToken");
        assert_eq!(code("AKIAUNKNOWN"), "InvalidClientTokenId");
    }

    #[test]
    fn dispatch_config_new_defaults_to_off() {
        let cfg = DispatchConfig::new("us-east-1", "123456789012");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.account_id, "123456789012");
        assert!(!cfg.verify_sigv4);
        assert_eq!(cfg.iam_mode, IamMode::Off);
    }

    /// An evaluator whose identity-only decision is fixed, and whose ceiling
    /// allows unless the SCPs (any `scps` given) or the session policies
    /// (any given) are set to deny.
    struct IdentityOnly {
        identity: crate::auth::IamDecision,
        scp_allows: bool,
        session_allows: bool,
        boundary_allows: bool,
    }

    impl IamPolicyEvaluator for IdentityOnly {
        fn evaluate(
            &self,
            _: &Principal,
            _: &crate::auth::IamAction,
            _: &ConditionContext,
            _: &[String],
            _: Option<&[String]>,
        ) -> crate::auth::IamDecision {
            self.identity
        }

        fn evaluate_ceiling(
            &self,
            _: &Principal,
            _: &crate::auth::IamAction,
            _: &ConditionContext,
            session_policies: &[String],
            scps: Option<&[String]>,
            with_boundary: bool,
        ) -> crate::auth::IamDecision {
            let scp_ok = scps.is_none() || self.scp_allows;
            let session_ok = session_policies.is_empty() || self.session_allows;
            let boundary_ok = !with_boundary || self.boundary_allows;
            if scp_ok && session_ok && boundary_ok {
                crate::auth::IamDecision::Allow
            } else {
                crate::auth::IamDecision::ImplicitDeny
            }
        }

        fn evaluate_with_resource_policy(
            &self,
            _: &Principal,
            _: &crate::auth::IamAction,
            _: &ConditionContext,
            _: Option<&str>,
            _: &str,
            _: &[String],
            _: Option<&[String]>,
        ) -> crate::auth::IamDecision {
            crate::auth::IamDecision::ImplicitDeny
        }
    }

    /// A provider whose grants match every request the same way.
    struct Grants(Option<crate::auth::GrantAuthorization>);

    impl ResourcePolicyProvider for Grants {
        fn resource_policy(&self, _: &str, _: &str) -> Option<String> {
            None
        }

        fn grant_authorization(
            &self,
            _: &str,
            _: &AwsRequest,
            _: &crate::auth::IamAction,
            _: &Principal,
        ) -> Option<crate::auth::GrantAuthorization> {
            self.0
        }
    }

    /// A grant naming the caller authorizes it on its own; one naming its
    /// account needs the caller's identity policies to allow the action too.
    #[test]
    fn grants_authorize_their_grantee_and_delegate_to_its_account() {
        use crate::auth::{GrantAuthorization, IamDecision};
        let request = AwsRequest {
            service: "kms".into(),
            action: "Decrypt".into(),
            region: "us-east-1".into(),
            account_id: "222222222222".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let action = crate::auth::IamAction {
            service: "kms",
            action: "Decrypt",
            resource: "arn:aws:kms:us-east-1:111111111111:key/k".into(),
        };
        let principal = Principal {
            arn: "arn:aws:iam::222222222222:user/u".into(),
            user_id: "AIDA".into(),
            account_id: "222222222222".into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let session = vec!["{}".to_string()];
        let scps = vec!["{}".to_string()];
        let check = |grant: Option<GrantAuthorization>,
                     evaluator: IdentityOnly,
                     provided: bool,
                     session_policies: &[String],
                     scps: Option<&[String]>| {
            let provider = Grants(grant);
            grant_authorizes(
                provided.then_some(&provider as &dyn ResourcePolicyProvider),
                &evaluator,
                "kms",
                &request,
                &action,
                &principal,
                &ConditionContext::default(),
                session_policies,
                scps,
            )
        };
        let open = |identity| IdentityOnly {
            identity,
            scp_allows: true,
            session_allows: true,
            boundary_allows: true,
        };
        let authorizes =
            |grant, identity, provided| check(grant, open(identity), provided, &[], None);
        let grantee = Some(GrantAuthorization::Grantee);
        let role = Some(GrantAuthorization::GranteeRole);
        let account = Some(GrantAuthorization::Account);
        assert!(authorizes(grantee, IamDecision::ImplicitDeny, true));
        assert!(authorizes(role, IamDecision::ImplicitDeny, true));
        assert!(authorizes(account, IamDecision::Allow, true));
        assert!(!authorizes(account, IamDecision::ImplicitDeny, true));
        assert!(!authorizes(None, IamDecision::Allow, true));
        assert!(!authorizes(grantee, IamDecision::Allow, false));

        // An SCP that does not allow the action caps every grant match.
        let scp_denies = || IdentityOnly {
            identity: IamDecision::ImplicitDeny,
            scp_allows: false,
            session_allows: true,
            boundary_allows: true,
        };
        for grant in [grantee, role] {
            assert!(!check(grant, scp_denies(), true, &[], Some(&scps)));
        }
        // Session policies cap a grant to the session's role, not one naming
        // the session itself.
        let session_denies = || IdentityOnly {
            identity: IamDecision::ImplicitDeny,
            scp_allows: true,
            session_allows: false,
            boundary_allows: true,
        };
        assert!(!check(role, session_denies(), true, &session, None));
        assert!(check(grantee, session_denies(), true, &session, None));
        // So does the role's permissions boundary; a grant naming the user
        // or session itself is not capped by a boundary.
        let boundary_denies = || IdentityOnly {
            identity: IamDecision::ImplicitDeny,
            scp_allows: true,
            session_allows: true,
            boundary_allows: false,
        };
        assert!(!check(role, boundary_denies(), true, &[], None));
        assert!(check(grantee, boundary_denies(), true, &[], None));
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

    /// Lambda answers an error with its code in `x-amzn-ErrorType` and a
    /// body shaped by the error's Smithy members, as recorded; other
    /// restJson1 services keep their body but get the header too.
    #[tokio::test]
    async fn lambda_errors_carry_x_amzn_errortype_and_lambdas_body() {
        let detected = protocol::DetectedRequest {
            service: "lambda".to_string(),
            action: String::new(),
            protocol: AwsProtocol::RestJson,
        };
        let resp = build_error_response(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Function not found: f",
            "req-3",
            ErrorEnvelope::for_request(&detected, &http::HeaderMap::new()),
        );
        assert_eq!(
            resp.headers()["x-amzn-errortype"],
            "ResourceNotFoundException"
        );
        assert_eq!(resp.headers()["content-type"], "application/json");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"Type": "User", "Message": "Function not found: f"})
        );

        let other = protocol::DetectedRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            protocol: AwsProtocol::RestJson,
        };
        let resp = build_error_response(
            StatusCode::NOT_FOUND,
            "NotFoundException",
            "Invalid API identifier specified",
            "req-4",
            ErrorEnvelope::for_request(&other, &http::HeaderMap::new()),
        );
        assert_eq!(resp.headers()["x-amzn-errortype"], "NotFoundException");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["__type"], "NotFoundException");
    }

    #[test]
    fn lambda_refuses_an_unknown_access_key_as_unrecognized_client() {
        let lambda = protocol::DetectedRequest {
            service: "lambda".to_string(),
            action: String::new(),
            protocol: AwsProtocol::RestJson,
        };
        let envelope = ErrorEnvelope::for_request(&lambda, &http::HeaderMap::new());
        assert_eq!(
            invalid_token_error(&envelope),
            (
                "UnrecognizedClientException",
                "The security token included in the request is invalid."
            )
        );
        assert_eq!(
            invalid_token_error(&ErrorEnvelope::from(AwsProtocol::Query)).0,
            "InvalidClientTokenId"
        );
    }

    fn rest_detected(service: &str) -> protocol::DetectedRequest {
        protocol::DetectedRequest {
            service: service.to_string(),
            action: String::new(),
            protocol: AwsProtocol::Rest,
        }
    }

    async fn body_string(resp: Response<Body>) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn cloudfront_and_route53_errors_use_error_response_wrapper() {
        for (service, ns) in [
            (
                "cloudfront",
                "http://cloudfront.amazonaws.com/doc/2020-05-31/",
            ),
            ("route53", "https://route53.amazonaws.com/doc/2013-04-01/"),
        ] {
            let resp = build_error_response(
                StatusCode::NOT_FOUND,
                "NoSuchThing",
                "missing",
                "req-w",
                ErrorEnvelope::for_request(&rest_detected(service), &http::HeaderMap::new()),
            );
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
            assert_eq!(
                resp.headers().get("x-amz-error-code").unwrap(),
                "NoSuchThing"
            );
            let body = body_string(resp).await;
            assert!(
                body.contains(&format!(
                    "<ErrorResponse xmlns=\"{ns}\"><Error><Type>Sender</Type>\
                     <Code>NoSuchThing</Code><Message>missing</Message></Error>\
                     <RequestId>req-w</RequestId></ErrorResponse>"
                )),
                "{service}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn s3_errors_keep_bare_error_document() {
        let resp = build_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "missing",
            "req-s3",
            ErrorEnvelope::for_request(&rest_detected("s3"), &http::HeaderMap::new()),
        );
        let body = body_string(resp).await;
        assert!(!body.contains("<ErrorResponse"), "{body}");
        assert!(body.contains("<Error>"), "{body}");
        assert!(body.contains("<Code>NoSuchBucket</Code>"), "{body}");
    }

    #[tokio::test]
    async fn s3_control_errors_use_error_response_wrapper() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "host",
            "000000000000.s3-control.us-east-1.amazonaws.com"
                .parse()
                .unwrap(),
        );
        let resp = build_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchAccessPoint",
            "missing",
            "req-ctl",
            ErrorEnvelope::for_request(&rest_detected("s3"), &headers),
        );
        assert_eq!(
            resp.headers().get("x-amz-error-code").unwrap(),
            "NoSuchAccessPoint"
        );
        let body = body_string(resp).await;
        assert!(
            body.contains(
                "<ErrorResponse><Error><Code>NoSuchAccessPoint</Code>\
                 <Message>missing</Message></Error>\
                 <RequestId>req-ctl</RequestId></ErrorResponse>"
            ),
            "{body}"
        );
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
    fn anonymous_s3_probe_finds_a_bucket_on_a_china_server() {
        // Answers only for ARNs the S3 policy provider's bucket parser reads
        // (`arn:aws:s3:::<bucket>`), so a probe the real provider would not
        // understand misses here too.
        struct RecordingProvider(parking_lot::Mutex<Vec<String>>);
        impl crate::auth::ResourcePolicyProvider for RecordingProvider {
            fn resource_policy(&self, _service: &str, _resource_arn: &str) -> Option<String> {
                None
            }
            fn resource_owner_account(&self, _service: &str, resource_arn: &str) -> Option<String> {
                self.0.lock().push(resource_arn.to_string());
                resource_arn
                    .strip_prefix("arn:aws:s3:::")
                    .filter(|bucket| *bucket == "my-bucket")
                    .map(|_| "000000000000".to_string())
            }
        }
        let provider = Arc::new(RecordingProvider(parking_lot::Mutex::new(Vec::new())));
        let mut cfg = DispatchConfig::new("cn-north-1", "000000000000");
        cfg.resource_policy_provider = Some(provider.clone());
        let uri: http::Uri = "/my-bucket/key.txt".parse().unwrap();
        assert_eq!(
            anonymous_s3_bucket(&uri, &cfg),
            Some("my-bucket".to_string())
        );
        assert_eq!(
            *provider.0.lock(),
            vec!["arn:aws:s3:::my-bucket".to_string()]
        );
    }

    /// A registry with an `s3` service and an `apigateway` one that claims
    /// unsigned requests whose first path segment is `prod` (a deployed stage).
    fn unsigned_routing_registry(with_s3: bool) -> ServiceRegistry {
        struct Named(&'static str);
        #[async_trait::async_trait]
        impl crate::service::AwsService for Named {
            fn service_name(&self) -> &str {
                self.0
            }
            async fn handle(
                &self,
                _req: AwsRequest,
            ) -> Result<crate::service::AwsResponse, crate::service::AwsServiceError> {
                unreachable!("routing only")
            }
            fn supported_actions(&self) -> &[&str] {
                &[]
            }
            fn claims_unsigned_request(
                &self,
                _method: &http::Method,
                raw_path: &str,
                _headers: &http::HeaderMap,
                account_id: &str,
            ) -> bool {
                assert_eq!(account_id, "000000000000");
                self.0 == "apigateway" && raw_path.starts_with("/prod/")
            }
        }
        let mut registry = ServiceRegistry::new();
        registry.register(Arc::new(Named("apigateway")));
        if with_s3 {
            registry.register(Arc::new(Named("s3")));
        }
        registry
    }

    #[test]
    fn unsigned_path_style_requests_go_to_s3_unless_api_gateway_claims_them() {
        let registry = unsigned_routing_registry(true);
        let cfg = DispatchConfig::new("us-east-1", "000000000000");
        let is_s3 =
            |method: http::Method, uri: &str, headers: &http::HeaderMap, query: &[(&str, &str)]| {
                let query: HashMap<String, String> = query
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                unsigned_request_is_s3_path_style(
                    &method,
                    &uri.parse().unwrap(),
                    headers,
                    &query,
                    &registry,
                    &cfg,
                )
            };
        let none = http::HeaderMap::new();
        // Missing bucket, any method, and the bare root (ListBuckets).
        assert!(is_s3(http::Method::GET, "/ghost-bucket/k.txt", &none, &[]));
        assert!(is_s3(http::Method::PUT, "/ghost-bucket/k.txt", &none, &[]));
        assert!(is_s3(http::Method::PUT, "/ghost-bucket", &none, &[]));
        assert!(is_s3(http::Method::GET, "/", &none, &[]));
        // A plain query string (not a presign) keeps it unsigned.
        assert!(is_s3(
            http::Method::GET,
            "/ghost-bucket",
            &none,
            &[("list-type", "2")]
        ));
        // API Gateway claims its invocation.
        assert!(!is_s3(http::Method::GET, "/prod/items", &none, &[]));
        // fakecloud endpoints are never S3.
        assert!(!is_s3(http::Method::GET, "/_fakecloud/health", &none, &[]));
        // Signed or presigned requests keep the catch-all (an IAM-auth API
        // signed for `execute-api` must reach API Gateway).
        let mut signed = http::HeaderMap::new();
        signed.insert(
            http::header::AUTHORIZATION,
            "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/execute-api/aws4_request"
                .parse()
                .unwrap(),
        );
        assert!(!is_s3(http::Method::GET, "/ghost-bucket/k", &signed, &[]));
        assert!(!is_s3(
            http::Method::GET,
            "/ghost-bucket/k",
            &none,
            &[(
                "X-Amz-Credential",
                "AKID/20240101/us-east-1/execute-api/aws4_request"
            )]
        ));
        assert!(!is_s3(
            http::Method::GET,
            "/ghost-bucket/k",
            &none,
            &[
                ("AWSAccessKeyId", "AKID"),
                ("Signature", "x"),
                ("Expires", "1")
            ]
        ));

        // Object-level PUTs (which stream) vs bucket-level ones.
        assert!(is_s3_object_put(&http::Method::PUT, "/b/k", &none));
        assert!(!is_s3_object_put(&http::Method::PUT, "/b/", &none));
        assert!(!is_s3_object_put(&http::Method::GET, "/b/k", &none));
        let vhost = {
            let mut h = http::HeaderMap::new();
            h.insert("host", "b.s3.us-east-1.amazonaws.com".parse().unwrap());
            h
        };
        assert!(is_s3_object_put(&http::Method::PUT, "/k", &vhost));
        assert!(is_unsigned(&none, &HashMap::new()));
        assert!(!is_unsigned(&signed, &HashMap::new()));
        // An incomplete SigV2 presign is not anonymous.
        assert!(!is_s3(
            http::Method::GET,
            "/",
            &none,
            &[("AWSAccessKeyId", "AKID"), ("Signature", "x")]
        ));

        // Without an S3 service registered nothing is routed to it.
        let registry = unsigned_routing_registry(false);
        assert!(!unsigned_request_is_s3_path_style(
            &http::Method::GET,
            &"/ghost-bucket/k".parse().unwrap(),
            &none,
            &HashMap::new(),
            &registry,
            &cfg,
        ));
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
    fn s3_routing_path_prefixes_the_host_bucket() {
        assert_eq!(s3_routing_path("/", Some("b")), "/b");
        assert_eq!(s3_routing_path("", Some("b")), "/b");
        assert_eq!(s3_routing_path("/k.txt", Some("b")), "/b/k.txt");
        assert_eq!(s3_routing_path("/dir/k", Some("a.b")), "/a.b/dir/k");
        assert_eq!(s3_routing_path("/b/k", None), "/b/k");
    }

    #[test]
    fn s3_routing_path_keeps_a_key_that_starts_with_the_bucket_name() {
        // The wire path is the whole key on a virtual-hosted request, so a key
        // whose first segment equals the bucket name must keep it.
        assert_eq!(
            s3_routing_path("/docs/intro.html", Some("docs")),
            "/docs/docs/intro.html"
        );
        assert_eq!(s3_routing_path("/docs", Some("docs")), "/docs/docs");
    }

    #[test]
    fn streaming_route_path_style_create_bucket_with_trailing_slash_skipped() {
        // The AWS SDKs send CreateBucket as `PUT /<bucket>/`. The trailing
        // slash must not make it look like an object upload: the body carries
        // `CreateBucketConfiguration` (location constraint, tag set) and has
        // to be buffered before IAM enforcement reads it.
        let headers = s3_sigv4_headers();
        assert_eq!(
            streaming_route(&http::Method::PUT, "/my-bucket/", &headers, &HashMap::new(),),
            None,
        );
    }

    #[test]
    fn streaming_route_path_style_doubled_slash_skipped() {
        // Routing filters empty path segments, so `PUT /<bucket>//` is a
        // bucket-level operation, not an object upload with a "/" key. The
        // streaming gate has to agree, or the body goes unbuffered for a
        // request the handler reads as CreateBucket / PutBucketTagging.
        let headers = s3_sigv4_headers();
        assert_eq!(
            streaming_route(
                &http::Method::PUT,
                "/my-bucket//",
                &headers,
                &HashMap::new()
            ),
            None,
        );
    }

    #[test]
    fn streaming_route_path_style_key_with_trailing_slash_streams() {
        // A real object key that ends in '/' (a directory marker) still
        // streams -- the gate rejects an absent key, not a trailing slash.
        let headers = s3_sigv4_headers();
        assert_eq!(
            streaming_route(
                &http::Method::PUT,
                "/my-bucket/folder/",
                &headers,
                &HashMap::new(),
            ),
            Some(("s3", "")),
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
    fn streaming_route_virtual_hosted_path_naming_the_bucket_streams() {
        // Under virtual-hosted addressing the whole path is the key, as on
        // real S3: `PUT /my-bucket` on `Host: my-bucket.s3...` uploads the
        // object `my-bucket`, not a CreateBucket, so every one of these
        // streams.
        let mut headers = s3_sigv4_headers();
        headers.insert(
            "host",
            "my-bucket.s3.us-east-1.amazonaws.com".parse().unwrap(),
        );
        for path in ["/my-bucket", "/my-bucket/", "/my-bucket/key.txt"] {
            assert_eq!(
                streaming_route(&http::Method::PUT, path, &headers, &HashMap::new()),
                Some(("s3", "")),
                "{path}",
            );
        }
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
    fn hoist_presigned_query_headers_skips_auth_params_and_keeps_direct_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-amz-meta-color", "red".parse().unwrap());
        let query: HashMap<String, String> = [
            (
                "X-Amz-Credential",
                "AKID/20260101/us-east-1/s3/aws4_request",
            ),
            ("X-Amz-Signature", "00"),
            ("X-Amz-Security-Token", "tok"),
            ("x-amz-meta-color", "blue"),
            ("X-Amz-Meta-Shape", "round"),
            ("x-amz-meta-name", "café"),
            ("x-amz-tagging", "env=test"),
            ("response-content-type", "text/plain"),
            ("partNumber", "1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        hoist_presigned_query_headers(&mut headers, &query);

        assert_eq!(headers["x-amz-meta-color"], "red");
        assert_eq!(headers["x-amz-meta-shape"], "round");
        assert_eq!(headers["x-amz-meta-name"], "=?UTF-8?B?Y2Fmw6k=?=");
        assert_eq!(headers["x-amz-tagging"], "env=test");
        for absent in [
            "x-amz-credential",
            "x-amz-signature",
            "x-amz-security-token",
            "response-content-type",
            "partnumber",
        ] {
            assert!(headers.get(absent).is_none(), "{absent}");
        }
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

    /// Root of account B reaching a bucket account A owns: allowed only when
    /// A's resource policy grants it, as for any cross-account principal.
    /// Root in its own account, and resources no provider claims, keep the
    /// root exemption.
    #[test]
    fn root_cross_account_needs_the_resource_policy() {
        use crate::auth::IamAction;
        use crate::service::{AwsResponse, AwsServiceError};
        struct OwnerProvider;
        impl crate::auth::ResourcePolicyProvider for OwnerProvider {
            fn resource_policy(&self, _service: &str, resource_arn: &str) -> Option<String> {
                resource_arn
                    .starts_with("arn:aws:s3:::granted")
                    .then(|| "granting-policy".to_string())
            }
            fn resource_owner_account(&self, _service: &str, resource_arn: &str) -> Option<String> {
                let bucket = resource_arn.strip_prefix("arn:aws:s3:::")?;
                let bucket = bucket.split('/').next()?;
                match bucket {
                    "own" => Some("222222222222".to_string()),
                    "granted" | "denied" => Some("111111111111".to_string()),
                    _ => None,
                }
            }
        }
        struct PolicyEvaluator(parking_lot::Mutex<Vec<ConditionContext>>);
        impl IamPolicyEvaluator for PolicyEvaluator {
            fn evaluate(
                &self,
                _: &Principal,
                _: &IamAction,
                _: &ConditionContext,
                _: &[String],
                _: Option<&[String]>,
            ) -> crate::auth::IamDecision {
                crate::auth::IamDecision::ImplicitDeny
            }
            fn evaluate_with_resource_policy(
                &self,
                _: &Principal,
                _: &IamAction,
                _: &ConditionContext,
                _: Option<&str>,
                _: &str,
                _: &[String],
                _: Option<&[String]>,
            ) -> crate::auth::IamDecision {
                crate::auth::IamDecision::ImplicitDeny
            }
            fn evaluate_resource_policy_only(
                &self,
                _: &Principal,
                _: &IamAction,
                context: &ConditionContext,
                policy: Option<&str>,
            ) -> crate::auth::IamDecision {
                self.0.lock().push(context.clone());
                if policy == Some("granting-policy") {
                    crate::auth::IamDecision::Allow
                } else {
                    crate::auth::IamDecision::ImplicitDeny
                }
            }
        }
        struct BucketService;
        #[async_trait::async_trait]
        impl crate::service::AwsService for BucketService {
            fn service_name(&self) -> &str {
                "s3"
            }
            async fn handle(&self, _: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
                unreachable!()
            }
            fn supported_actions(&self) -> &[&str] {
                &[]
            }
            fn iam_action_for(&self, request: &AwsRequest) -> Option<IamAction> {
                Some(IamAction {
                    service: "s3",
                    action: "GetObject",
                    resource: format!("arn:aws:s3:::{}", request.path_segments.join("/")),
                })
            }
        }
        let root = Principal {
            arn: "arn:aws:iam::222222222222:root".to_string(),
            user_id: "222222222222".to_string(),
            account_id: "222222222222".to_string(),
            principal_type: PrincipalType::Root,
            source_identity: None,
            tags: None,
        };
        let detected = protocol::DetectedRequest {
            service: "s3".to_string(),
            action: String::new(),
            protocol: AwsProtocol::Rest,
        };
        let request = |bucket: &str| AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "222222222222".to_string(),
            request_id: "req".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![bucket.to_string(), "key".to_string()],
            raw_path: format!("/{bucket}/key"),
            raw_query: String::new(),
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: Some("AKIAROOTB".to_string()),
            principal: Some(root.clone()),
        };
        struct Org;
        impl crate::auth::ScpResolver for Org {
            fn scps_for(&self, _: &Principal) -> Option<Vec<String>> {
                None
            }
            fn principal_org(&self, account: &str) -> Option<(String, String)> {
                (account == "222222222222").then(|| ("o-abc".to_string(), "o-abc/r-1/".to_string()))
            }
        }
        let issued = chrono::Utc::now();
        let credential = crate::auth::ResolvedCredential {
            secret_access_key: "secret".to_string(),
            session_token: Some("token".to_string()),
            principal: root.clone(),
            session_policies: Vec::new(),
            mfa_present: true,
            token_issued_at: Some(issued),
            federated_provider: None,
            role_arn: None,
        };
        let evaluator = PolicyEvaluator(parking_lot::Mutex::new(Vec::new()));
        let run = |bucket: &str, mode: IamMode| {
            let mut cfg = DispatchConfig::new("us-east-1", "111111111111");
            cfg.iam_mode = mode;
            cfg.resource_policy_provider = Some(Arc::new(OwnerProvider));
            cfg.scp_resolver = Some(Arc::new(Org));
            authorize_root_cross_account(
                &root,
                &BucketService,
                &request(bucket),
                &evaluator,
                &cfg,
                &detected,
                "req",
                None,
                Some(&credential),
            )
        };
        let denied = run("denied", IamMode::Strict).expect("no grant must deny under strict");
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        assert!(run("granted", IamMode::Strict).is_none());
        assert!(run("own", IamMode::Strict).is_none());
        assert!(run("unclaimed", IamMode::Strict).is_none());
        assert!(run("denied", IamMode::Soft).is_none());

        // The root path evaluates with the same condition context as any
        // other principal: the owner's account, the principal's organization
        // and the session keys its credential carries.
        let contexts = evaluator.0.lock();
        assert!(!contexts.is_empty());
        for ctx in contexts.iter() {
            assert_eq!(
                ctx.lookup("aws:ResourceAccount"),
                Some(vec!["111111111111".into()])
            );
            assert_eq!(ctx.lookup("aws:PrincipalOrgID"), Some(vec!["o-abc".into()]));
            assert_eq!(
                ctx.lookup("aws:PrincipalOrgPaths"),
                Some(vec!["o-abc/r-1/".into()])
            );
            assert_eq!(ctx.aws_mfa_present, Some(true));
            assert_eq!(ctx.aws_token_issue_time, Some(issued));
            assert_eq!(ctx.aws_principal_type.as_deref(), Some("Account"));
        }
    }

    #[test]
    fn denial_messages_name_the_scp_when_it_refused() {
        use crate::auth::IamDecision;
        assert_eq!(scp_denial_reason(IamDecision::Allow, "s3:GetObject"), None);
        // The SCP layer is named only when it caused the denial: its own
        // explicit Deny, or not allowing when nothing else explicitly denied.
        use IamDecision::{Allow, ExplicitDeny, ImplicitDeny};
        assert_eq!(scp_cause(ExplicitDeny, ExplicitDeny), Some(ExplicitDeny));
        assert_eq!(scp_cause(ImplicitDeny, ImplicitDeny), Some(ImplicitDeny));
        assert_eq!(scp_cause(ExplicitDeny, ImplicitDeny), None);
        assert_eq!(scp_cause(ImplicitDeny, Allow), None);
        assert_eq!(scp_cause(ExplicitDeny, Allow), None);
        // The denial code follows the protocol: awsQuery `AccessDenied`,
        // JSON/REST `AccessDeniedException`, with S3 and SNS their own.
        assert_eq!(access_denied_code("s3", AwsProtocol::Rest), "AccessDenied");
        assert_eq!(
            access_denied_code("sts", AwsProtocol::Query),
            "AccessDenied"
        );
        assert_eq!(
            access_denied_code("iam", AwsProtocol::Query),
            "AccessDenied"
        );
        assert_eq!(
            access_denied_code("elasticloadbalancing", AwsProtocol::Query),
            "AccessDenied"
        );
        assert_eq!(
            access_denied_code("sqs", AwsProtocol::Query),
            "AccessDenied"
        );
        assert_eq!(
            access_denied_code("sqs", AwsProtocol::Json),
            "AccessDeniedException"
        );
        assert_eq!(
            access_denied_code("sns", AwsProtocol::Query),
            "AuthorizationError"
        );
        assert_eq!(
            access_denied_code("kms", AwsProtocol::Json),
            "AccessDeniedException"
        );
        assert_eq!(
            access_denied_code("lambda", AwsProtocol::Rest),
            "AccessDeniedException"
        );
        // SQS's JSON path carries the awsQuery code in `x-amzn-query-error`.
        let detected = |service: &str, protocol| protocol::DetectedRequest {
            service: service.to_string(),
            action: "SendMessage".to_string(),
            protocol,
        };
        let headers = http::HeaderMap::new();
        let sqs_json =
            access_denied_response("m", "r", &detected("sqs", AwsProtocol::Json), &headers);
        assert_eq!(
            sqs_json.headers()["x-amzn-query-error"],
            "AccessDenied;Sender"
        );
        let kms = access_denied_response("m", "r", &detected("kms", AwsProtocol::Json), &headers);
        assert!(kms.headers().get("x-amzn-query-error").is_none());
        let explicit = scp_denial_reason(IamDecision::ExplicitDeny, "s3:GetObject");
        let implicit = scp_denial_reason(IamDecision::ImplicitDeny, "s3:GetObject");
        let user = "arn:aws:iam::222222222222:user/u";
        assert_eq!(
            not_authorized_message(user, "s3:GetObject", "arn:aws:s3:::b/k", explicit.as_deref(), None),
            "User: arn:aws:iam::222222222222:user/u is not authorized to perform: s3:GetObject on resource: arn:aws:s3:::b/k with an explicit deny in a service control policy"
        );
        assert_eq!(
            not_authorized_message(user, "s3:GetObject", "arn:aws:s3:::b/k", implicit.as_deref(), Some("BLOB")),
            "User: arn:aws:iam::222222222222:user/u is not authorized to perform: s3:GetObject on resource: arn:aws:s3:::b/k because no service control policy allows the s3:GetObject action Encoded authorization failure message: BLOB"
        );
        assert_eq!(
            not_authorized_message(user, "s3:GetObject", "arn:aws:s3:::b/k", None, Some("BLOB")),
            "User: arn:aws:iam::222222222222:user/u is not authorized to perform: s3:GetObject on resource: arn:aws:s3:::b/k Encoded authorization failure message: BLOB"
        );
    }

    /// SCPs cap a member account's root user as they cap every principal of
    /// the account: in its own account, and across accounts even where the
    /// resource policy grants it. No SCPs (the management account, no
    /// organization) leave root as it was.
    #[tokio::test]
    async fn root_requests_are_capped_by_scps() {
        use crate::auth::IamAction;
        use crate::service::{AwsResponse, AwsServiceError};
        struct OwnerProvider;
        impl crate::auth::ResourcePolicyProvider for OwnerProvider {
            fn resource_policy(&self, _service: &str, _resource_arn: &str) -> Option<String> {
                Some("granting-policy".to_string())
            }
            fn resource_owner_account(&self, _service: &str, resource_arn: &str) -> Option<String> {
                let bucket = resource_arn.strip_prefix("arn:aws:s3:::")?;
                match bucket.split('/').next()? {
                    "own" => Some("222222222222".to_string()),
                    _ => Some("111111111111".to_string()),
                }
            }
        }
        /// Allows what the resource policy grants; the ceiling allows when
        /// there are SCPs and every one is `allow`, and an SCP `deny` denies
        /// explicitly.
        struct PolicyEvaluator(parking_lot::Mutex<Vec<ConditionContext>>);
        impl IamPolicyEvaluator for PolicyEvaluator {
            fn evaluate(
                &self,
                _: &Principal,
                _: &IamAction,
                _: &ConditionContext,
                _: &[String],
                _: Option<&[String]>,
            ) -> crate::auth::IamDecision {
                crate::auth::IamDecision::ImplicitDeny
            }
            fn evaluate_with_resource_policy(
                &self,
                _: &Principal,
                _: &IamAction,
                _: &ConditionContext,
                _: Option<&str>,
                _: &str,
                _: &[String],
                _: Option<&[String]>,
            ) -> crate::auth::IamDecision {
                crate::auth::IamDecision::ImplicitDeny
            }
            fn evaluate_resource_policy_only(
                &self,
                _: &Principal,
                _: &IamAction,
                _: &ConditionContext,
                policy: Option<&str>,
            ) -> crate::auth::IamDecision {
                if policy == Some("granting-policy") {
                    crate::auth::IamDecision::Allow
                } else {
                    crate::auth::IamDecision::ImplicitDeny
                }
            }
            fn evaluate_ceiling(
                &self,
                _: &Principal,
                _: &IamAction,
                context: &ConditionContext,
                session_policies: &[String],
                scps: Option<&[String]>,
                with_boundary: bool,
            ) -> crate::auth::IamDecision {
                assert!(session_policies.is_empty() && !with_boundary);
                self.0.lock().push(context.clone());
                let scps = scps.unwrap_or_default();
                if scps.iter().any(|s| s == "deny") {
                    crate::auth::IamDecision::ExplicitDeny
                } else if !scps.is_empty() && scps.iter().all(|s| s == "allow") {
                    crate::auth::IamDecision::Allow
                } else {
                    crate::auth::IamDecision::ImplicitDeny
                }
            }
        }
        struct BucketService;
        #[async_trait::async_trait]
        impl crate::service::AwsService for BucketService {
            fn service_name(&self) -> &str {
                "s3"
            }
            async fn handle(&self, _: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
                unreachable!()
            }
            fn supported_actions(&self) -> &[&str] {
                &[]
            }
            fn iam_action_for(&self, request: &AwsRequest) -> Option<IamAction> {
                Some(IamAction {
                    service: "s3",
                    action: "GetObject",
                    resource: format!("arn:aws:s3:::{}", request.path_segments.join("/")),
                })
            }
        }
        struct Org(Option<Vec<String>>);
        impl crate::auth::ScpResolver for Org {
            fn scps_for(&self, _: &Principal) -> Option<Vec<String>> {
                self.0.clone()
            }
            fn principal_org(&self, _: &str) -> Option<(String, String)> {
                Some(("o-abc".to_string(), "o-abc/r-1/".to_string()))
            }
        }
        let root = Principal {
            arn: "arn:aws:iam::222222222222:root".to_string(),
            user_id: "222222222222".to_string(),
            account_id: "222222222222".to_string(),
            principal_type: PrincipalType::Root,
            source_identity: None,
            tags: None,
        };
        let detected = protocol::DetectedRequest {
            service: "s3".to_string(),
            action: String::new(),
            protocol: AwsProtocol::Rest,
        };
        let request = |bucket: &str| AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "222222222222".to_string(),
            request_id: "req".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![bucket.to_string(), "key".to_string()],
            raw_path: format!("/{bucket}/key"),
            raw_query: String::new(),
            method: http::Method::GET,
            is_query_protocol: false,
            access_key_id: Some("ASIAROOTB".to_string()),
            principal: Some(root.clone()),
        };
        let evaluator = PolicyEvaluator(parking_lot::Mutex::new(Vec::new()));
        let run = |bucket: &str, scps: Option<&[&str]>, mode: IamMode| {
            let mut cfg = DispatchConfig::new("us-east-1", "111111111111");
            cfg.iam_mode = mode;
            cfg.resource_policy_provider = Some(Arc::new(OwnerProvider));
            cfg.scp_resolver = Some(Arc::new(Org(
                scps.map(|s| s.iter().map(|d| d.to_string()).collect())
            )));
            authorize_root(
                &root,
                &BucketService,
                &request(bucket),
                &evaluator,
                &cfg,
                &detected,
                "req",
                None,
                None,
            )
        };
        async fn message(resp: Response<Body>) -> String {
            assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(body.to_vec()).unwrap()
        }
        for bucket in ["own", "granted"] {
            // No SCPs apply (management account, no organization).
            assert!(run(bucket, None, IamMode::Strict).is_none(), "{bucket}");
            // SCPs allowing the action.
            assert!(
                run(bucket, Some(&["allow"]), IamMode::Strict).is_none(),
                "{bucket}"
            );
            // An SCP denying the action, explicitly or by not allowing it.
            let explicit = message(
                run(bucket, Some(&["allow", "deny"]), IamMode::Strict)
                    .expect("an SCP deny must refuse root"),
            )
            .await;
            assert!(
                explicit.contains("<Code>AccessDenied</Code>")
                    && explicit.contains(&format!(
                        "<Message>User: arn:aws:iam::222222222222:root is not authorized to perform: s3:GetObject on resource: arn:aws:s3:::{bucket}/key with an explicit deny in a service control policy</Message>"
                    )),
                "{explicit}"
            );
            let implicit = message(
                run(bucket, Some(&["narrow"]), IamMode::Strict)
                    .expect("an SCP not allowing the action must refuse root"),
            )
            .await;
            assert!(
                implicit.contains("<Code>AccessDenied</Code>")
                    && implicit.contains(&format!(
                        "<Message>User: arn:aws:iam::222222222222:root is not authorized to perform: s3:GetObject on resource: arn:aws:s3:::{bucket}/key because no service control policy allows the s3:GetObject action</Message>"
                    )),
                "{implicit}"
            );
            assert!(
                run(bucket, Some(&[]), IamMode::Strict).is_some(),
                "{bucket}"
            );
            // Soft mode logs and lets the request through.
            assert!(
                run(bucket, Some(&["deny"]), IamMode::Soft).is_none(),
                "{bucket}"
            );
        }

        // An operation with no IAM action mapping cannot be checked against
        // the SCPs: under strict it fails closed, as for any principal.
        struct UnmappedService;
        #[async_trait::async_trait]
        impl crate::service::AwsService for UnmappedService {
            fn service_name(&self) -> &str {
                "s3"
            }
            async fn handle(&self, _: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
                unreachable!()
            }
            fn supported_actions(&self) -> &[&str] {
                &[]
            }
            fn iam_action_for(&self, _: &AwsRequest) -> Option<IamAction> {
                None
            }
        }
        let unmapped = |scps: Option<&[&str]>, mode: IamMode| {
            let mut cfg = DispatchConfig::new("us-east-1", "111111111111");
            cfg.iam_mode = mode;
            cfg.resource_policy_provider = Some(Arc::new(OwnerProvider));
            cfg.scp_resolver = Some(Arc::new(Org(
                scps.map(|s| s.iter().map(|d| d.to_string()).collect())
            )));
            authorize_root(
                &root,
                &UnmappedService,
                &request("own"),
                &evaluator,
                &cfg,
                &detected,
                "req",
                None,
                None,
            )
        };
        let refused = message(
            unmapped(Some(&["allow"]), IamMode::Strict)
                .expect("an unmapped operation under SCPs must fail closed"),
        )
        .await;
        assert!(
            refused.contains("<Code>AccessDenied</Code>")
                && refused.contains("no IAM action mapping exists for this operation"),
            "{refused}"
        );
        assert!(unmapped(Some(&["allow"]), IamMode::Soft).is_none());
        // No SCPs to apply: root needs no identity policy, so nothing to
        // check.
        assert!(unmapped(None, IamMode::Strict).is_none());
        // The SCPs see the request's global keys: the resource's owner and
        // the caller's organization.
        let contexts = evaluator.0.lock();
        assert!(contexts
            .iter()
            .any(|c| c.lookup("aws:ResourceAccount") == Some(vec!["111111111111".into()])));
        assert!(contexts
            .iter()
            .any(|c| c.lookup("aws:ResourceAccount") == Some(vec!["222222222222".into()])));
        for ctx in contexts.iter() {
            assert_eq!(ctx.lookup("aws:PrincipalOrgID"), Some(vec!["o-abc".into()]));
            assert_eq!(
                ctx.aws_principal_arn.as_deref(),
                Some("arn:aws:iam::222222222222:root")
            );
        }
    }
}

/// Whether the request is addressed to an API Gateway execute-api host
/// (`{api-id}.execute-api.{region}.amazonaws.com`, or a local alias of it).
fn is_execute_api_host(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|host| host.contains(".execute-api."))
}

#[cfg(test)]
mod execute_api_host_tests {
    use super::is_execute_api_host;

    #[test]
    fn recognizes_execute_api_hosts_only() {
        let mut h = http::HeaderMap::new();
        h.insert(
            http::header::HOST,
            "abc123.execute-api.us-east-1.amazonaws.com"
                .parse()
                .unwrap(),
        );
        assert!(is_execute_api_host(&h));
        h.insert(http::header::HOST, "localhost:4566".parse().unwrap());
        assert!(!is_execute_api_host(&h));
        assert!(!is_execute_api_host(&http::HeaderMap::new()));
    }
}
