//! `S3Service` `cors` family — extracted from service.rs by audit-2026-05-19.

use super::*;
use crate::service::{parse_cors_config, strip_xml_comments};

const MALFORMED_XML: &str =
    "The XML you provided was not well-formed or did not validate against our published schema";

/// Validate a CORS configuration body, returning the AWS `(code, message)` for
/// the first problem found.
///
/// Shared so every writer of `cors_config` enforces the same rules: a config
/// that reaches storage without passing here can be one that matches no request
/// at all, leaving the bucket silently CORS-dead.
pub(crate) fn validate_cors_xml(body_str: &str) -> Result<(), (&'static str, String)> {
    // Count and parse over the same comment-free text. A `<CORSRule>` inside
    // an `<!-- ... -->` is not a rule, and counting it while the parser skips
    // it would reject a config real S3 accepts.
    let scannable = strip_xml_comments(body_str);

    // Validate CORS configuration
    let rule_count = scannable.matches("<CORSRule>").count();
    if rule_count == 0 || rule_count > 100 {
        return Err(("MalformedXML", MALFORMED_XML.to_string()));
    }

    // `parse_cors_config` stops at the first `<CORSRule>` it cannot close,
    // returning only the rules it managed to parse. Requiring the counts to
    // agree turns an unterminated rule into `MalformedXML` instead of a
    // stored config whose tail was silently dropped.
    // The raw body, not `scannable`: `parse_cors_config` strips comments
    // itself, and at request time it strips the stored raw body exactly once.
    // Handing it pre-stripped text would strip twice here and once there, so a
    // body whose stripping synthesizes a new `<!--` out of overlapping
    // delimiters would validate against different markup than it later runs on.
    let parsed = parse_cors_config(body_str);
    if parsed.len() != rule_count {
        return Err(("MalformedXML", MALFORMED_XML.to_string()));
    }

    // Scanned per rule rather than across the whole document: an
    // `<AllowedMethod>` or `<MaxAgeSeconds>` sitting outside any `<CORSRule>`
    // feeds no rule, and `parse_cors_config` ignores it, so rejecting the body
    // over it would refuse a config whose live semantics are fine.
    let rule_bodies: Vec<&str> = {
        let mut bodies = Vec::new();
        let mut rest = scannable.as_str();
        while let Some(start) = rest.find("<CORSRule>") {
            let after = &rest[start + 10..];
            match after.find("</CORSRule>") {
                Some(end) => {
                    bodies.push(&after[..end]);
                    rest = &after[end + 11..];
                }
                None => break,
            }
        }
        bodies
    };

    // Validate HTTP methods
    let valid_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"];
    for rule_body in &rule_bodies {
        let mut remaining = *rule_body;
        while let Some(start) = remaining.find("<AllowedMethod>") {
            let after = &remaining[start + 15..];
            if let Some(end) = after.find("</AllowedMethod>") {
                let method = after[..end].trim();
                // An empty element has no name to report, so it falls through to
                // the required-member check below and gets `MalformedXML` like
                // every other empty required element, rather than an
                // "Unsupported method is " with nothing after it.
                if !method.is_empty() && !valid_methods.contains(&method) {
                    return Err(("InvalidRequest", format!("Found unsupported HTTP method in CORS config. Unsupported method is {method}")));
                }
                remaining = &after[end + 16..];
            } else {
                // Opening tag without a matching closer is malformed
                // XML; reject instead of saving a half-parsed config.
                return Err(("MalformedXML", MALFORMED_XML.to_string()));
            }
        }
    }

    // `AllowedMethods` and `AllowedOrigins` are both required members of
    // `CORSRule`. A rule missing either matches nothing at request time, so
    // accepting one would leave the bucket silently CORS-dead rather than
    // telling the caller their config is wrong — AWS rejects it outright.
    // `parse_cors_config` drops empty values, so a list left empty here means
    // the rule carried nothing usable — whether the tag was absent or present
    // but blank. A stray blank tag beside a real value is not rejected, since
    // the rule is still live. Runs after the method validation above so an
    // unsupported `<AllowedMethod>` still gets the AWS-shaped error naming the
    // offending value.
    for rule in parsed {
        if rule.allowed_methods.is_empty() || rule.allowed_origins.is_empty() {
            return Err(("MalformedXML", MALFORMED_XML.to_string()));
        }

        // AWS allows at most one `*` per AllowedOrigin / AllowedHeader.
        // Both are deny gates at request time, and the matchers read only
        // the first `*`, so a second one would be treated as a literal and
        // quietly make the rule match nothing. Reject at write time, where
        // the caller can still see which value is wrong.
        for (label, values) in [
            ("AllowedOrigin", &rule.allowed_origins),
            ("AllowedHeader", &rule.allowed_headers),
        ] {
            if let Some(bad) = values.iter().find(|v| v.matches('*').count() > 1) {
                return Err((
                    "InvalidRequest",
                    format!("{label} \"{bad}\" can not have more than one wildcard."),
                ));
            }
        }

        // S3 supports no wildcard at all in ExposeHeader. Storing one would
        // work locally — Fetch reads a literal `*` as "expose all" for an
        // uncredentialed request — and then be rejected by real AWS.
        if let Some(bad) = rule.expose_headers.iter().find(|v| v.contains('*')) {
            return Err(("InvalidRequest", format!("ExposeHeader \"{bad}\" contains wildcard. We currently do not support wildcard for ExposeHeader.")));
        }

        // `ExposeHeader` and `AllowedOrigin` are echoed into response headers,
        // and `set_cors_header` silently drops a value that will not parse, so
        // a control character would strip the header from every response with
        // no error anywhere. `AllowedHeader` is only ever matched against, not
        // echoed — the allow-headers echo comes from the request — but a value
        // that cannot be a header name matches nothing, so it is rejected here
        // too rather than left to fail silently at request time.
        for (label, values) in [
            ("AllowedHeader", &rule.allowed_headers),
            ("ExposeHeader", &rule.expose_headers),
            ("AllowedOrigin", &rule.allowed_origins),
        ] {
            if let Some(bad) = values
                .iter()
                .find(|v| v.parse::<http::HeaderValue>().is_err())
            {
                return Err((
                    "InvalidRequest",
                    format!("{label} \"{bad}\" is not a valid header value."),
                ));
            }
        }
    }

    // A non-numeric `MaxAgeSeconds` parses to `None`, so the config would
    // round-trip looking healthy while every preflight silently shipped
    // without `Access-Control-Max-Age` and browsers re-preflighted each
    // request. Reject it instead, as S3 does.
    for rule_body in &rule_bodies {
        let mut remaining = *rule_body;
        while let Some(start) = remaining.find("<MaxAgeSeconds>") {
            let after = &remaining[start + 15..];
            let Some(end) = after.find("</MaxAgeSeconds>") else {
                return Err(("MalformedXML", MALFORMED_XML.to_string()));
            };
            if after[..end].trim().parse::<u32>().is_err() {
                return Err(("MalformedXML", MALFORMED_XML.to_string()));
            }
            remaining = &after[end + 16..];
        }
    }

    Ok(())
}

impl S3Service {
    // ---- CORS ----

    pub(crate) fn put_bucket_cors(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();

        let mut accts = self.state.write();

        // Existence is checked before `get_or_create` so a rejected request
        // leaves nothing behind: `get_or_create` would insert an empty state
        // for an unknown account even when the body then fails validation.
        if !accts
            .get(account_id)
            .is_some_and(|s| s.buckets.contains_key(bucket))
        {
            return Err(no_such_bucket(bucket));
        }

        // After the bucket lookup: a missing bucket is `NoSuchBucket`, not a
        // complaint about the config. Otherwise a run that races bucket
        // creation sends the operator to debug a config that is fine.
        validate_cors_xml(&body_str).map_err(|(code, message)| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, message)
        })?;

        let b = accts
            .get_or_create(account_id)
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.cors_config = Some(body_str.clone());
        self.store
            .put_bucket_subresource(account_id, bucket, BucketSubresource::Cors, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_cors(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.cors_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    pub(crate) fn delete_bucket_cors(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.cors_config = None;
        self.store
            .delete_bucket_subresource(account_id, bucket, BucketSubresource::Cors)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
