//! S3 POST Object — the browser-form "POST Policy" upload flow targeted by
//! `generate_presigned_post` (boto3) / `createPresignedPost` (JS SDK).
//!
//! Unlike every other S3 write, the caller never signs the HTTP request
//! itself: a trusted party (the presigning caller) hands the browser a
//! base64 JSON "policy" document plus a signature over that document, and
//! the browser POSTs both — along with the object key/metadata and the file
//! bytes — as a single `multipart/form-data` body straight to the bucket
//! root. There is no `Authorization` header and no signed query string, so
//! this endpoint parses its own signature material out of the form fields
//! instead of going through [`fakecloud_aws::sigv4::verify`].

use std::collections::HashMap;

use base64::Engine as _;
use http::Method;

use fakecloud_core::auth::is_root_bypass;

use super::*;

/// One parsed multipart form field: original (as-submitted) name + value.
/// Lookups against the policy's conditions are case-insensitive per the S3
/// POST Policy spec, so callers normally match on `name.to_ascii_lowercase()`.
struct FormField {
    name: String,
    value: String,
}

struct ParsedForm {
    fields: Vec<FormField>,
    file_name: Option<String>,
    file_content_type: Option<String>,
    file_bytes: Bytes,
    file_present: bool,
}

impl ParsedForm {
    fn field_lower(&self, lower_name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(lower_name))
            .map(|f| f.value.as_str())
    }
}

fn invalid_argument(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgument", message)
}

fn access_denied(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "AccessDenied", message)
}

fn signature_does_not_match(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::FORBIDDEN, "SignatureDoesNotMatch", message)
}

async fn multer_field_bytes(field: multer::Field<'_>) -> Result<Bytes, AwsServiceError> {
    field
        .bytes()
        .await
        .map_err(|err| invalid_argument(format!("failed to read multipart field body: {err}")))
}

/// Parse the `multipart/form-data` body into form fields + the `file` part.
/// Field names carry no ordering guarantee from the caller's perspective —
/// we just collect whatever we're handed, matching real S3's tolerance for
/// field order (aside from `file` needing to be read after the fields that
/// govern it, which multer's streaming parser naturally enforces since the
/// browser always places `file` last).
async fn parse_multipart_form(
    content_type: &str,
    body: Bytes,
) -> Result<ParsedForm, AwsServiceError> {
    let boundary = multer::parse_boundary(content_type)
        .map_err(|_| invalid_argument("POST Object requires a multipart/form-data body"))?;

    let stream = futures_util::stream::once(async move { Ok::<Bytes, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);

    let mut fields = Vec::new();
    let mut file_name = None;
    let mut file_content_type = None;
    let mut file_bytes = Bytes::new();
    let mut file_present = false;

    loop {
        let next = multipart
            .next_field()
            .await
            .map_err(|err| invalid_argument(format!("malformed multipart body: {err}")))?;
        let Some(field) = next else { break };

        let name = field.name().unwrap_or("").to_string();
        if name.eq_ignore_ascii_case("file") {
            file_present = true;
            file_name = field.file_name().map(|s| s.to_string());
            file_content_type = field.content_type().map(|m| m.to_string());
            file_bytes = multer_field_bytes(field).await?;
        } else {
            let value = field
                .text()
                .await
                .map_err(|err| invalid_argument(format!("malformed multipart field: {err}")))?;
            fields.push(FormField { name, value });
        }
    }

    Ok(ParsedForm {
        fields,
        file_name,
        file_content_type,
        file_bytes,
        file_present,
    })
}

/// Resolve the `key` form field, substituting the S3-documented
/// `${filename}` placeholder with the uploaded part's filename.
fn resolve_key(key_field: &str, file_name: Option<&str>) -> String {
    if key_field.contains("${filename}") {
        key_field.replace("${filename}", file_name.unwrap_or_default())
    } else {
        key_field.to_string()
    }
}

/// A single parsed POST-Policy condition. S3's policy document allows two
/// shapes: an exact-match object `{"field": "value"}` and a 3-element array
/// `["op", "$field", value_or_prefix]` (or `["content-length-range", min,
/// max]`, which has no field name).
enum PolicyCondition {
    Eq { field: String, value: String },
    StartsWith { field: String, prefix: String },
    ContentLengthRange { min: u64, max: u64 },
}

fn parse_conditions(policy: &serde_json::Value) -> Result<Vec<PolicyCondition>, AwsServiceError> {
    let raw = policy
        .get("conditions")
        .and_then(|c| c.as_array())
        .ok_or_else(|| invalid_argument("policy document is missing a `conditions` array"))?;

    let mut out = Vec::with_capacity(raw.len());
    for cond in raw {
        if let Some(obj) = cond.as_object() {
            // Exact-match object form: `{"field": "value"}`.
            for (field, value) in obj {
                let value = value
                    .as_str()
                    .ok_or_else(|| invalid_argument("policy condition value must be a string"))?
                    .to_string();
                out.push(PolicyCondition::Eq {
                    field: field.trim_start_matches('$').to_ascii_lowercase(),
                    value,
                });
            }
        } else if let Some(arr) = cond.as_array() {
            if arr.len() != 3 {
                return Err(invalid_argument(
                    "policy condition array must have exactly 3 elements",
                ));
            }
            let op = arr[0]
                .as_str()
                .ok_or_else(|| invalid_argument("policy condition operator must be a string"))?;
            match op {
                "eq" => {
                    let field = arr[1]
                        .as_str()
                        .ok_or_else(|| invalid_argument("policy condition field must be a string"))?
                        .trim_start_matches('$')
                        .to_ascii_lowercase();
                    let value = arr[2]
                        .as_str()
                        .ok_or_else(|| invalid_argument("policy condition value must be a string"))?
                        .to_string();
                    out.push(PolicyCondition::Eq { field, value });
                }
                "starts-with" => {
                    let field = arr[1]
                        .as_str()
                        .ok_or_else(|| invalid_argument("policy condition field must be a string"))?
                        .trim_start_matches('$')
                        .to_ascii_lowercase();
                    let prefix = arr[2]
                        .as_str()
                        .ok_or_else(|| {
                            invalid_argument("policy condition prefix must be a string")
                        })?
                        .to_string();
                    out.push(PolicyCondition::StartsWith { field, prefix });
                }
                "content-length-range" => {
                    let min = arr[1].as_u64().ok_or_else(|| {
                        invalid_argument("content-length-range min must be a number")
                    })?;
                    let max = arr[2].as_u64().ok_or_else(|| {
                        invalid_argument("content-length-range max must be a number")
                    })?;
                    out.push(PolicyCondition::ContentLengthRange { min, max });
                }
                other => {
                    return Err(invalid_argument(format!(
                        "unsupported policy condition operator: {other}"
                    )));
                }
            }
        } else {
            return Err(invalid_argument(
                "policy condition must be an object or a 3-element array",
            ));
        }
    }
    Ok(out)
}

/// Look up the submitted value for a condition's field name (lowercased),
/// consulting the well-known derived fields (`bucket`, `key`, `file`'s
/// resolved content-type) before falling back to the raw submitted form
/// fields.
fn submitted_value<'a>(
    field: &str,
    bucket: &'a str,
    resolved_key: &'a str,
    content_type: &'a str,
    form: &'a ParsedForm,
) -> Option<&'a str> {
    match field {
        "bucket" => Some(bucket),
        "key" => Some(resolved_key),
        "content-type" => Some(content_type),
        other => form.field_lower(other),
    }
}

fn enforce_conditions(
    conditions: &[PolicyCondition],
    bucket: &str,
    resolved_key: &str,
    content_type: &str,
    file_size: u64,
    form: &ParsedForm,
) -> Result<(), AwsServiceError> {
    for cond in conditions {
        match cond {
            PolicyCondition::Eq { field, value } => {
                let actual = submitted_value(field, bucket, resolved_key, content_type, form);
                if actual != Some(value.as_str()) {
                    return Err(access_denied(format!(
                        "Policy Condition failed: [\"eq\", \"${field}\", \"{value}\"]"
                    )));
                }
            }
            PolicyCondition::StartsWith { field, prefix } => {
                let actual = submitted_value(field, bucket, resolved_key, content_type, form);
                if !actual.is_some_and(|v| v.starts_with(prefix.as_str())) {
                    return Err(access_denied(format!(
                        "Policy Condition failed: [\"starts-with\", \"${field}\", \"{prefix}\"]"
                    )));
                }
            }
            PolicyCondition::ContentLengthRange { min, max } => {
                if file_size < *min {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "EntityTooSmall",
                        "Your proposed upload is smaller than the minimum allowed size",
                    ));
                }
                if file_size > *max {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "EntityTooLarge",
                        "Your proposed upload exceeds the maximum allowed size",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// `x-amz-credential` is `{access_key}/{date}/{region}/{service}/aws4_request`.
struct ParsedCredential {
    access_key: String,
    date_stamp: String,
    region: String,
    service: String,
}

fn parse_credential(raw: &str) -> Result<ParsedCredential, AwsServiceError> {
    let parts: Vec<&str> = raw.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(invalid_argument("malformed x-amz-credential"));
    }
    Ok(ParsedCredential {
        access_key: parts[0].to_string(),
        date_stamp: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    })
}

impl S3Service {
    /// `POST /{bucket}` with a `multipart/form-data` body — S3 POST Object
    /// (browser-form "POST Policy" upload). See module docs for the wire
    /// contract.
    pub(crate) async fn post_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let content_type = req
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();

        // The bucket must already exist (mirrors PutObject's NoSuchBucket
        // check); do this before spending effort parsing the multipart body.
        {
            let accounts = self.state.read();
            let empty = crate::state::S3State::new(account_id, &req.region);
            let state = accounts.get(account_id).unwrap_or(&empty);
            if !state.buckets.contains_key(bucket) {
                return Err(no_such_bucket(bucket));
            }
        }

        let form = parse_multipart_form(&content_type, req.body.clone()).await?;

        let key_field = form
            .field_lower("key")
            .ok_or_else(|| invalid_argument("POST Object requires a `key` form field"))?;
        let resolved_key = resolve_key(key_field, form.file_name.as_deref());
        if resolved_key.is_empty() {
            return Err(invalid_argument(
                "POST Object requires a non-empty resolved key",
            ));
        }
        if !form.file_present {
            return Err(invalid_argument("POST Object requires a `file` form field"));
        }

        let object_content_type = form
            .field_lower("content-type")
            .map(|s| s.to_string())
            .or_else(|| form.file_content_type.clone())
            .unwrap_or_else(|| "binary/octet-stream".to_string());

        // --- Policy validation ---
        let policy_b64 = form
            .field_lower("policy")
            .ok_or_else(|| invalid_argument("POST Object requires a `policy` form field"))?
            .to_string();
        let policy_bytes = base64::engine::general_purpose::STANDARD
            .decode(policy_b64.as_bytes())
            .map_err(|_| invalid_argument("`policy` is not valid base64"))?;
        let policy: serde_json::Value = serde_json::from_slice(&policy_bytes)
            .map_err(|_| invalid_argument("`policy` is not valid JSON"))?;

        if let Some(expiration) = policy.get("expiration").and_then(|v| v.as_str()) {
            let expires_at = chrono::DateTime::parse_from_rfc3339(expiration)
                .map_err(|_| invalid_argument("policy `expiration` is not valid RFC3339"))?
                .with_timezone(&Utc);
            if Utc::now() > expires_at {
                return Err(access_denied(
                    "Invalid according to Policy: Policy expired.",
                ));
            }
        }

        let conditions = parse_conditions(&policy)?;
        enforce_conditions(
            &conditions,
            bucket,
            &resolved_key,
            &object_content_type,
            form.file_bytes.len() as u64,
            &form,
        )?;

        // --- Signature verification ---
        let algorithm = form.field_lower("x-amz-algorithm").unwrap_or_default();
        if !algorithm.is_empty() && !algorithm.eq_ignore_ascii_case("AWS4-HMAC-SHA256") {
            return Err(invalid_argument(format!(
                "unsupported x-amz-algorithm: {algorithm}"
            )));
        }
        let credential_raw = form.field_lower("x-amz-credential").ok_or_else(|| {
            invalid_argument("POST Object requires an `x-amz-credential` form field")
        })?;
        let credential = parse_credential(credential_raw)?;
        let signature = form.field_lower("x-amz-signature").ok_or_else(|| {
            invalid_argument("POST Object requires an `x-amz-signature` form field")
        })?;

        if is_root_bypass(&credential.access_key) {
            // Matches the codebase-wide `test*` root-bypass convention
            // (`fakecloud_core::auth::is_root_bypass`): local-dev root
            // credentials always pass, signature not cryptographically
            // checked.
        } else {
            let resolver = self.credential_resolver.as_ref().ok_or_else(|| {
                signature_does_not_match(
                    "The request signature we calculated does not match the signature you provided",
                )
            })?;
            let resolved = resolver.resolve(&credential.access_key).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "InvalidAccessKeyId",
                    "The AWS Access Key Id you provided does not exist in our records.",
                )
            })?;
            let expected = fakecloud_aws::sigv4::sign_string(
                &resolved.secret_access_key,
                &credential.date_stamp,
                &credential.region,
                &credential.service,
                &policy_b64,
            );
            if !expected.eq_ignore_ascii_case(signature) {
                return Err(signature_does_not_match(
                    "The request signature we calculated does not match the signature you provided",
                ));
            }
        }

        // --- Store the object via the existing PutObject sink ---
        let mut synth_headers = HeaderMap::new();
        synth_headers.insert(
            http::header::CONTENT_TYPE,
            object_content_type
                .parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("binary/octet-stream")),
        );
        // Forward any other AWS-meaningful fields the caller supplied
        // (storage class, ACL, user metadata, cache/content headers) as if
        // they were the equivalent PutObject headers — POST Object form
        // field names match their header counterparts verbatim.
        for f in &form.fields {
            let lower = f.name.to_ascii_lowercase();
            let forwardable = lower == "cache-control"
                || lower == "content-disposition"
                || lower == "content-encoding"
                || lower == "content-language"
                || lower == "expires"
                || lower.starts_with("x-amz-");
            if !forwardable {
                continue;
            }
            if let (Ok(name), Ok(value)) = (
                lower.parse::<http::header::HeaderName>(),
                f.value.parse::<http::HeaderValue>(),
            ) {
                synth_headers.insert(name, value);
            }
        }

        let synth_req = AwsRequest {
            service: "s3".to_string(),
            action: "PutObject".to_string(),
            region: req.region.clone(),
            account_id: account_id.to_string(),
            request_id: req.request_id.clone(),
            headers: synth_headers,
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(Some(axum::body::Body::from(
                form.file_bytes.clone(),
            ))),
            path_segments: vec![bucket.to_string(), resolved_key.clone()],
            raw_path: format!("/{bucket}/{resolved_key}"),
            raw_query: String::new(),
            method: Method::PUT,
            is_query_protocol: false,
            access_key_id: req.access_key_id.clone(),
            principal: req.principal.clone(),
        };

        let put_response = self
            .put_object(account_id, &synth_req, bucket, &resolved_key)
            .await?;
        let etag = put_response
            .headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("\"\"")
            .to_string();

        // --- Build the POST Object response ---
        let success_status = form
            .field_lower("success_action_status")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(204);

        let mut headers = HeaderMap::new();
        headers.insert(
            "etag",
            etag.parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("\"\"")),
        );

        match success_status {
            200 => Ok(AwsResponse {
                status: StatusCode::OK,
                content_type: String::new(),
                body: Bytes::new().into(),
                headers,
            }),
            201 => {
                let location = format!("/{bucket}/{resolved_key}");
                headers.insert(
                    "location",
                    location
                        .parse()
                        .unwrap_or_else(|_| http::HeaderValue::from_static("/")),
                );
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></PostResponse>",
                    crate::service::xml_escape(&location),
                    crate::service::xml_escape(bucket),
                    crate::service::xml_escape(&resolved_key),
                    crate::service::xml_escape(&etag),
                );
                Ok(AwsResponse {
                    status: StatusCode::CREATED,
                    content_type: "application/xml".to_string(),
                    body: Bytes::from(body).into(),
                    headers,
                })
            }
            _ => Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: String::new(),
                body: Bytes::new().into(),
                headers,
            }),
        }
    }
}
