//! The shared tail of S3 responses: CORS headers and access logging, for
//! every routed request and for the refusals S3 answers like one.

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{
    find_cors_rule, origin_matches, parse_cors_config, request_key, route, set_cors_header,
    single_cors_header, S3Service, CORS_VARY,
};
use crate::logging;

impl S3Service {
    /// How a request the classification refuses is answered: the refusal,
    /// with the bucket's CORS headers and an access-log record when S3 gets
    /// that far with it ([`route::Refusal::reaches_bucket`]).
    pub(super) fn refuse(&self, req: &AwsRequest, refusal: route::Refusal) -> AwsServiceError {
        if !refusal.reaches_bucket {
            return refusal.error;
        }
        let bucket = req.path_segments.first().map(String::as_str);
        let owner = bucket.and_then(|b| self.bucket_owner_account(b));
        let account_id = owner.as_deref().unwrap_or(&req.account_id);
        let mut result = Err(refusal.error);
        self.finish_response(
            req,
            account_id,
            bucket,
            request_key(req).as_deref(),
            None,
            &mut result,
        );
        match result {
            Err(error) => error,
            Ok(_) => unreachable!("a refusal stays an error"),
        }
    }

    /// The shared tail of every bucket-addressed response, success or error:
    /// the bucket's CORS headers and its server access log record.
    pub(super) fn finish_response(
        &self,
        req: &AwsRequest,
        account_id: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        op: Option<route::S3Op>,
        result: &mut Result<AwsResponse, AwsServiceError>,
    ) {
        // Capture origin for CORS response headers. A blank `Origin:` is
        // treated as absent — the request carries nothing to evaluate CORS
        // against, and letting it through would match a `*` rule and stamp
        // CORS headers onto a request no browser would have sent that way.
        // Same single-value read as the preflight path: a request carrying more
        // than one `Origin` is not a single origin to allow.
        let origin_header = single_cors_header(&req.headers, "origin").map(str::to_string);
        // Apply CORS headers to the response if Origin was present
        // Keyed on the raw header, not the usable value: an `Origin` that is
        // blank, unreadable, or split across conflicting lines still makes the
        // response origin-dependent, so it needs `Vary` even though it gets no
        // allow-origin. Skipping the block entirely would ship an ACAO-less
        // body with no `Vary`, which a shared cache then replays to a
        // legitimate origin — the hole this whole change exists to close.
        if let Some(b_name) = bucket.filter(|_| req.headers.contains_key("origin")) {
            let cors_config = {
                let accounts = self.state.read();
                let _empty_s3 = crate::state::S3State::new(account_id, &req.region);
                let state = accounts.get(account_id).unwrap_or(&_empty_s3);
                state
                    .buckets
                    .get(b_name)
                    .and_then(|b| b.cors_config.clone())
            };
            if let Some(ref config) = cors_config {
                // `Vary` goes on every response we evaluated CORS for, matching
                // rule or not: a disallowed origin gets no ACAO, and that
                // ACAO-less response must not be cached and replayed to an
                // allowed origin.
                set_cors_header(result, "vary", CORS_VARY);
                let rules = parse_cors_config(config);
                // The actual request is matched on method as well as origin, as
                // on S3: a rule allowing only GET must not hand an ACAO to a
                // DELETE from that origin, which would let the browser pass the
                // response to the page.
                // Empty requested-headers: only a preflight declares headers.
                // `origin_header` is `None` when the header was present but
                // unusable; `find_cors_rule` refuses an empty origin, so such a
                // request keeps the `Vary` above and gets no allow-origin.
                let origin = origin_header.as_deref().unwrap_or("");
                if let Some(rule) = find_cors_rule(&rules, origin, req.method.as_str(), &[]) {
                    // `*` only when the entry that actually matched is `*`. A rule
                    // listing both a concrete origin and `*` matches the
                    // concrete one first, and echoing `*` there would drop
                    // allow-credentials for an origin the rule names.
                    let matched_origin = if rule
                        .allowed_origins
                        .iter()
                        .find(|o| origin_matches(origin, o))
                        .is_some_and(|o| o == "*")
                    {
                        "*"
                    } else {
                        origin
                    };
                    // Errors get the allow-origin too: S3 answers a 404 for an
                    // allowed origin with ACAO, so a `mode: 'cors'` fetch sees
                    // a real 404 instead of an opaque CORS network error.
                    set_cors_header(result, "access-control-allow-origin", matched_origin);
                    if matched_origin != "*" {
                        set_cors_header(result, "access-control-allow-credentials", "true");
                    }
                    if !rule.expose_headers.is_empty() {
                        set_cors_header(
                            result,
                            "access-control-expose-headers",
                            &rule.expose_headers.join(", "),
                        );
                    }
                }
            }
        }

        // Write S3 access log entry if the source bucket has logging enabled
        if let Some(b_name) = bucket {
            let status_code = match &*result {
                Ok(resp) => resp.status.as_u16(),
                Err(e) => e.status().as_u16(),
            };
            let operation = route::access_log_operation(op)
                .unwrap_or_else(|| logging::operation_name(&req.method, key));
            logging::maybe_write_access_log(
                &self.state,
                &self.store,
                b_name,
                &logging::AccessLogRequest {
                    operation,
                    key,
                    status: status_code,
                    request_id: &req.request_id,
                    method: req.method.as_str(),
                    path: &req.raw_path,
                },
            );
        }
    }
}
