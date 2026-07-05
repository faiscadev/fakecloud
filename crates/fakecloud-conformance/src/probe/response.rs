//! probe `response` (audit-2026-05-19).

use super::*;

/// Classify the response when the variant expected a successful outcome.
///
/// Pre-`Expectation::Success` policy was: any 2xx-or-4xx == Pass. Reasoning
/// at the time was that synthetic placeholder inputs make a 4xx
/// (ResourceNotFoundException, ValidationException) the *expected* shape
/// for an implemented handler. The trade-off was that fakecloud's *own*
/// routing-miss 4xxs (returning 404 for a URL form the router didn't
/// know how to dispatch — exactly #817) were indistinguishable from
/// real handler-emitted 4xxs and slipped through as Pass.
///
/// New policy splits 4xx by what the body looks like:
/// - 4xx with an AWS-shaped error code in the body (`__type` JSON field
///   or `<Code>` XML element) — handler ran, returned a real exception.
///   If the op declares `error_shapes`, the code must short-name-match
///   one of them (Smithy declares which exceptions an op can raise).
///   Pass.
/// - 4xx with no recognisable AWS error code, OR with a code that's
///   absent from the op's declared `error_shapes` — likely fakecloud's
///   own routing-miss / unhandled-form response. Fail.
///
/// Net effect: signed routing reaches a handler -> Pass. Routing
/// silently misses (404 with body that doesn't match Smithy) -> Fail.
pub(super) fn classify_success_expectation(
    http_status: u16,
    body: &str,
    op_error_shapes: Option<&[String]>,
    service_name: &str,
) -> ProbeStatus {
    if (200..300).contains(&http_status) {
        return ProbeStatus::Pass;
    }
    if !(400..500).contains(&http_status) {
        return ProbeStatus::UnexpectedResult(format!(
            "Expected success, got HTTP {}",
            http_status
        ));
    }
    let code = match extract_aws_error_code(body) {
        Some(c) => c,
        None => {
            return ProbeStatus::UnexpectedResult(format!(
                "HTTP {} with no AWS error code in body (likely routing miss): {}",
                http_status,
                truncate(body, 200)
            ));
        }
    };
    // Some services publish "shared error responses" outside per-operation
    // Smithy `errors:` lists — codes the model implicitly inherits across
    // every op that touches the same resource family. S3 is the canonical
    // example: <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html>
    // documents `NoSuchBucket`, `NoSuchKey`, `InvalidBucketName`, and
    // `AccessDenied` as universal responses any bucket/object-taking op can
    // return, but the AWS Smithy file only enumerates them on a handful of
    // operations (`HeadBucket` -> NotFound, `CreateSession` -> NoSuchBucket,
    // etc.). Accepting these as handler-emitted errors here keeps the
    // classifier aligned with the published API contract rather than the
    // incomplete Smithy enumeration.
    if service_common_errors(service_name)
        .iter()
        .any(|c| *c == code)
    {
        return ProbeStatus::Pass;
    }
    // Op model available -> require the code to be in its declared errors.
    // Op model unavailable (no model for this service or unknown op) -> any
    // AWS-shaped error counts as a handler response.
    if let Some(declared) = op_error_shapes {
        // Empty declared list means the op declares *no* errors in its
        // Smithy contract — anything 4xx/5xx is undeclared, so don't
        // silently mark these as Pass.
        if !declared.is_empty() && matches_declared_error(&code, declared) {
            ProbeStatus::Pass
        } else {
            ProbeStatus::UnexpectedResult(format!(
                "HTTP {} with undeclared error '{}' (not in op's Smithy error_shapes)",
                http_status, code
            ))
        }
    } else {
        ProbeStatus::Pass
    }
}

/// Pull the AWS error code out of a response body. Handles all four
/// AWS wire forms:
///   - JSON: `{"__type":"X"}` or `{"__type":"com.amazonaws.svc#X"}`
///     (restJson1 / awsJson1.1)
///   - JSON: `{"code":"X"}` / `{"Code":"X"}` (Smithy fallbacks some
///     services use)
///   - XML: `<Error><Code>X</Code></Error>` (restXml — S3, CloudFront)
///   - XML: `<ErrorResponse><Error><Code>X</Code></Error></ErrorResponse>`
///     (awsQuery — IAM, RDS, SNS, ELB, CFN, STS)
///
/// Returns the short name (after `#` if present). Returns `None` when
/// the body has no recognisable AWS error code — the signal we care
/// about for distinguishing real handler responses from routing misses.
pub(super) fn extract_aws_error_code(body: &str) -> Option<String> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        for key in ["__type", "Code", "code"] {
            if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
                return Some(short_error_name(s));
            }
        }
        // Some restJson1 services nest under `Error` or `error`.
        for outer in ["Error", "error"] {
            if let Some(inner) = v.get(outer) {
                for key in ["Code", "code", "__type"] {
                    if let Some(s) = inner.get(key).and_then(|x| x.as_str()) {
                        return Some(short_error_name(s));
                    }
                }
            }
        }
    }
    // XML <Code>X</Code> — first occurrence wins.
    if let Some(start) = body.find("<Code>") {
        let after = &body[start + "<Code>".len()..];
        if let Some(end) = after.find("</Code>") {
            return Some(after[..end].trim().to_string());
        }
    }
    None
}

/// Strip Smithy namespace from an error type. `com.amazonaws.lambda#X` -> `X`.
pub(super) fn short_error_name(s: &str) -> String {
    let after_hash = s.rsplit('#').next().unwrap_or(s);
    // Some services prefix with shape namespace via colon syntax.
    let after_colon = after_hash.rsplit(':').next().unwrap_or(after_hash);
    after_colon.trim().to_string()
}

/// Service-wide error codes that AWS documents as "shared error responses"
/// applicable to any operation in the service, even when the per-op Smithy
/// `errors:` list omits them.
///
/// The list comes from each service's published API reference, not from
/// fakecloud's own behaviour. For services with no shared-error
/// documentation this returns an empty slice and the strict per-op rule
/// applies verbatim.
pub(super) fn service_common_errors(service_name: &str) -> &'static [&'static str] {
    match service_name {
        // S3 "Common Error Responses": every operation can return these on
        // a missing/forbidden bucket or object. Source:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        // Anything else (validation errors, conditional-write violations,
        // op-specific failures) still has to match the per-op `errors:` list.
        "s3" => &[
            "NoSuchBucket",
            "NoSuchKey",
            "InvalidBucketName",
            "AccessDenied",
            "NotFound",
        ],
        // EC2's Smithy model declares no per-operation `errors:` lists at all,
        // yet the EC2 Query API has a large, well-documented set of client
        // error codes every operation can return for a missing/invalid/
        // wrong-state resource. Source: EC2 API "Error codes" reference
        // (https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html).
        // These are real AWS responses to the synthetic ids/params the probes
        // send (a nonexistent instance -> InvalidInstanceID.NotFound, an
        // illegal transition -> IncorrectInstanceState, etc.), so a handler
        // returning one of them ran correctly. Anything outside this list is
        // still treated as a routing miss / undeclared error.
        "ec2" => &[
            "InvalidInstanceID.NotFound",
            "InvalidInstanceID.Malformed",
            "IncorrectInstanceState",
            "InstanceLimitExceeded",
            "InvalidParameterValue",
            "InvalidParameterCombination",
            "InvalidParameter",
            "MissingParameter",
            "InvalidAMIID.NotFound",
            "InvalidAMIID.Malformed",
            "InvalidAMIID.Unavailable",
        ],
        // EKS under-declares two client errors that the real API returns for
        // sub-resource operations:
        //   - ResourceNotFoundException: any op naming a parent cluster that
        //     doesn't exist. The model declares it on most sub-resource ops but
        //     omits it from CreateNodegroup / CreateFargateProfile even though
        //     AWS returns it for a missing cluster.
        //   - ResourceInUseException: creating a sub-resource whose name is
        //     already taken. The model declares it on CreateNodegroup but omits
        //     it from CreateFargateProfile, which returns the same 409 for a
        //     duplicate profile name.
        // A handler returning either to the probes' synthetic inputs ran
        // correctly. Source: EKS API reference (CreateNodegroup /
        // CreateFargateProfile "Errors").
        "eks" => &["ResourceNotFoundException", "ResourceInUseException"],
        // STS returns AccessDenied (HTTP 403) whenever a role can't be assumed
        // -- the role doesn't exist, or its trust policy rejects the caller --
        // for AssumeRole / AssumeRoleWithWebIdentity / AssumeRoleWithSAML. AWS
        // documents this as a standard response, but the per-op Smithy `errors:`
        // lists only enumerate the token/policy exceptions, not AccessDenied. A
        // handler returning AccessDenied to the probes' synthetic (nonexistent)
        // role ARNs ran correctly. Source: STS API "Common Errors" + per-op docs.
        "sts" => &["AccessDenied"],
        // CodeConnections' CreateConnection / CreateHost require the caller to
        // resolve a provider type: CreateConnection needs a ProviderType or a
        // HostArn to inherit one from, and both reject malformed input. AWS
        // returns InvalidInputException (the service's canonical input-validation
        // error, defined in the model and enumerated on its repository-link /
        // sync operations) for these, but the connection/host operations'
        // per-op `errors:` lists omit it even though the real API returns it for
        // a connection created with neither ProviderType nor HostArn. A handler
        // returning it to the probes' synthetic inputs ran correctly.
        "codeconnections" => &["InvalidInputException"],
        // CodeArtifact's `ResourceNotFoundException` is a service-wide response:
        // AWS returns it for a missing domain/repository/package/version on
        // essentially every operation that dereferences one, but the model only
        // enumerates it on a subset (for example `DeleteDomain`, `ListDomains`,
        // and `ListRepositories` omit it even though the live API returns it
        // when the referenced domain does not exist). A handler returning it for
        // the probes' synthetic (non-existent) resources ran correctly.
        "codeartifact" => &["ResourceNotFoundException"],
        _ => &[],
    }
}

/// Strict per-op error matcher.
///
/// `declared` is the operation's directly-declared error wire codes — already
/// resolved upstream in `probe_variant_with_model` so each entry is what AWS
/// actually puts on the wire (`aws.protocols#awsQueryError.code` if present,
/// otherwise the shape's short name). An observed wire code passes only if
/// it appears verbatim in that list.
///
/// We deliberately do NOT accept:
/// - A universal "framework error" allowlist (ValidationException, Throttling,
///   AccessDenied, etc. — #1344). If the op declares those, they're already
///   in `declared`; if it doesn't, AWS doesn't return them for that op and
///   we shouldn't either.
/// - A `NoSuch*` / `*NotFound` blanket accept (#1347). Those codes belong to
///   specific shapes on specific ops; the strict matcher demands the model
///   actually declare them.
/// - Suffix stripping of `Exception` / `Fault` / `Exists` from shape names
///   (#1336). That was a heuristic substitute for parsing the awsQueryError
///   trait — now that we parse the trait directly, the heuristic is gone.
pub(super) fn matches_declared_error(code: &str, declared: &[String]) -> bool {
    declared.iter().any(|wire| wire == code)
}

pub(super) fn classify_response(
    variant_name: &str,
    http_status: u16,
    body: &str,
    expectation: &Expectation,
    duration_ms: u64,
    op_error_shapes: Option<&[String]>,
    service_name: &str,
) -> ProbeResult {
    // Classify as NotImplemented when fakecloud signals "we did not find a
    // handler for this action" — as opposed to AWS-shaped errors that mean
    // "handler found, rejected synthetic input" (e.g. ValidationException,
    // ResourceNotFoundException for a non-existent resource id).
    //
    // The error-body patterns below cover every way fakecloud services
    // today express an unrouted action:
    //   - `not implemented` / `NotImplemented` — `ActionNotImplemented`
    //     emitted by the generic service dispatcher.
    //   - `UnknownAction` / `InvalidAction` — Query-protocol services
    //     for unknown `Action=…` form params.
    //   - `UnknownOperationException` — Lambda for unrouted URL paths.
    //   - `Unknown path:` — API Gateway v2, EventBridge Scheduler, and
    //     a few other REST-routed services return this string in the
    //     error body when `resolve_action` yields None.
    //   - `Unknown operation:` — also emitted by some Query services.
    //
    // Important: these substrings must NOT appear in legitimate AWS-shaped
    // error responses for implemented actions. `NotFoundException` alone is
    // not listed here because it's also what implemented handlers return
    // for a missing resource id.
    let is_not_implemented = body.contains("not implemented")
        || body.contains("NotImplemented")
        || body.contains("UnknownAction")
        || body.contains("InvalidAction")
        || body.contains("UnknownOperationException")
        || body.contains("Unknown path:")
        || body.contains("Unknown operation:");

    if is_not_implemented {
        return ProbeResult {
            variant_name: variant_name.to_string(),
            status: ProbeStatus::NotImplemented,
            http_status,
            response_body: body.to_string(),
            duration_ms,
        };
    }

    if http_status == 500 {
        return ProbeResult {
            variant_name: variant_name.to_string(),
            status: ProbeStatus::Crash(format!("HTTP 500: {}", truncate(body, 200))),
            http_status,
            response_body: body.to_string(),
            duration_ms,
        };
    }

    let status = match expectation {
        Expectation::Success => {
            classify_success_expectation(http_status, body, op_error_shapes, service_name)
        }
        Expectation::AnyError => {
            if http_status >= 400 {
                ProbeStatus::Pass
            } else {
                ProbeStatus::UnexpectedResult(format!("Expected error, got HTTP {}", http_status))
            }
        }
        Expectation::Error(expected_code) => {
            // Don't accept a 2xx body that happens to contain the expected
            // error string — the response is only valid as Error if the
            // status code is actually an error status.
            if http_status >= 400 && body.contains(expected_code) {
                ProbeStatus::Pass
            } else if http_status >= 400 {
                ProbeStatus::UnexpectedResult(format!(
                    "Expected error '{}', got HTTP {} with different error",
                    expected_code, http_status
                ))
            } else {
                ProbeStatus::UnexpectedResult(format!(
                    "Expected error '{}', got HTTP {}",
                    expected_code, http_status
                ))
            }
        }
    };

    ProbeResult {
        variant_name: variant_name.to_string(),
        status,
        http_status,
        response_body: body.to_string(),
        duration_ms,
    }
}

pub(super) fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        // Find a char boundary at or before `max` to avoid panicking on multi-byte chars.
        let boundary = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= max)
            .last()
            .unwrap_or(0);
        &s[..boundary]
    }
}
