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

/// Reject any submitted form field that the signed policy does not authorize.
/// The whole POST-Policy security model is that the signed `conditions` list
/// enumerates exactly which fields (and values) the browser may post; real S3
/// rejects a request carrying a field with no matching condition with
/// `AccessDenied` "Invalid according to Policy: Extra input fields: X". A small
/// set of fields is exempt from needing a condition: the signature material
/// itself (`policy`, `x-amz-signature`, `x-amz-algorithm`, `x-amz-credential`,
/// `x-amz-date`, `x-amz-security-token`, and the SigV2 `awsaccesskeyid` /
/// `signature`), the `file` part, and any `x-ignore-*` field. Real S3 does not
/// require the signature-material fields to be enumerated in `conditions` (and
/// a forged `x-amz-credential` is caught by the signature check regardless).
/// `content-length-range` has no field name, so it authorizes nothing here (it
/// governs the file size, checked separately).
fn enforce_no_extra_fields(
    conditions: &[PolicyCondition],
    form: &ParsedForm,
) -> Result<(), AwsServiceError> {
    let mut authorized: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for cond in conditions {
        match cond {
            PolicyCondition::Eq { field, .. } => {
                authorized.insert(field.as_str());
            }
            PolicyCondition::StartsWith { field, .. } => {
                authorized.insert(field.as_str());
            }
            PolicyCondition::ContentLengthRange { .. } => {}
        }
    }
    for f in &form.fields {
        let lower = f.name.to_ascii_lowercase();
        let exempt = matches!(
            lower.as_str(),
            "policy"
                | "x-amz-signature"
                | "x-amz-algorithm"
                | "x-amz-credential"
                | "x-amz-date"
                | "x-amz-security-token"
                | "awsaccesskeyid"
                | "signature"
                | "file"
        ) || lower.starts_with("x-ignore-");
        if exempt {
            continue;
        }
        if !authorized.contains(lower.as_str()) {
            return Err(access_denied(format!(
                "Invalid according to Policy: Extra input fields: {lower}"
            )));
        }
    }
    Ok(())
}

/// The PutObject request headers (`PutObjectRequest`'s `@httpHeader`
/// members, lower-cased) a POST Object form field carries as-is. Left out:
/// `content-type` (the form's own field, set separately), `content-length`
/// (the file's), and `x-amz-acl` / `x-amz-tagging`, which a form sets with
/// its `acl` and `tagging` fields.
const PUT_OBJECT_FORM_HEADERS: &[&str] = &[
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-md5",
    "expires",
    "if-match",
    "if-none-match",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-md5",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-checksum-sha512",
    "x-amz-checksum-xxhash128",
    "x-amz-checksum-xxhash3",
    "x-amz-checksum-xxhash64",
    "x-amz-expected-bucket-owner",
    "x-amz-grant-full-control",
    "x-amz-grant-read",
    "x-amz-grant-read-acp",
    "x-amz-grant-write-acp",
    "x-amz-object-lock-event-hold",
    "x-amz-object-lock-event-hold-duration-days",
    "x-amz-object-lock-event-hold-duration-years",
    "x-amz-object-lock-legal-hold",
    "x-amz-object-lock-mode",
    "x-amz-object-lock-retain-until-date",
    "x-amz-request-payer",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-storage-class",
    "x-amz-website-redirect-location",
    "x-amz-write-offset-bytes",
];

fn malformed_xml() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published \
         schema",
    )
}

/// A POST Object `tagging` field's tag set: a well-formed `Tagging` document
/// whose every `Tag` has a key (else 400 MalformedXML), validated as any
/// object tag set (no duplicate or `aws:` keys, at most 10 tags).
fn tagging_document(xml: &str) -> Result<Vec<(String, String)>, AwsServiceError> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut root = None;
    let mut depth = 0usize;
    let mut tag_elements = 0usize;
    loop {
        match reader.read_event().map_err(|_| malformed_xml())? {
            Event::Start(e) => {
                let name = e.local_name().as_ref().to_vec();
                if depth == 0 {
                    if root.is_some() {
                        return Err(malformed_xml());
                    }
                    root = Some(name.clone());
                }
                if name == b"Tag" {
                    tag_elements += 1;
                }
                depth += 1;
            }
            Event::End(_) => depth = depth.saturating_sub(1),
            Event::Empty(e) if depth == 0 || e.local_name().as_ref() == b"Tag" => {
                return Err(malformed_xml());
            }
            Event::Text(t) if depth == 0 && !t.iter().all(u8::is_ascii_whitespace) => {
                return Err(malformed_xml());
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if depth != 0 || root.as_deref() != Some(b"Tagging".as_slice()) {
        return Err(malformed_xml());
    }
    let tags = crate::service::parse_tagging_xml(xml);
    if tags.len() != tag_elements {
        return Err(malformed_xml());
    }
    crate::service::validate_tags(&tags)?;
    if tags.len() > 10 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "Object tags cannot be greater than 10",
        ));
    }
    Ok(tags)
}

/// A tag set as the URL-encoded `key=value&...` PutObject's `x-amz-tagging`
/// carries.
fn tagging_header(tags: &[(String, String)]) -> String {
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    tags.iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                utf8_percent_encode(k, NON_ALPHANUMERIC),
                utf8_percent_encode(v, NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Absolute, path-style URL for a stored object, derived from the request's
/// `Host` header (matching fakecloud's path-style addressing). Falls back to a
/// root-relative path when no usable host is present. Used for the `Location`
/// response header / `PostResponse` XML and `success_action_redirect`.
fn object_url(req: &AwsRequest, bucket: &str, key: &str) -> String {
    match req
        .headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
    {
        Some(host) => format!("http://{host}/{bucket}/{key}"),
        None => format!("/{bucket}/{key}"),
    }
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

        // A form with no policy, credential or signature is an anonymous
        // upload, authorized on the bucket's own policy alone (S3 user guide,
        // "Browser-based uploads using POST": a POST without them is
        // anonymous and "requires that the bucket be publicly writable").
        let anonymous = ["policy", "x-amz-credential", "x-amz-signature"]
            .iter()
            .all(|name| form.field_lower(name).is_none());
        let signer = if anonymous {
            None
        } else {
            // --- Decode the policy document ---
            let policy_b64 = form
                .field_lower("policy")
                .ok_or_else(|| invalid_argument("POST Object requires a `policy` form field"))?
                .to_string();
            let policy_bytes = base64::engine::general_purpose::STANDARD
                .decode(policy_b64.as_bytes())
                .map_err(|_| invalid_argument("`policy` is not valid base64"))?;
            let policy: serde_json::Value = serde_json::from_slice(&policy_bytes)
                .map_err(|_| invalid_argument("`policy` is not valid JSON"))?;

            // --- Signature verification ---
            // Authenticate the policy signature *before* evaluating the policy's
            // conditions, matching real S3's precedence: a request that both
            // carries a bad signature and violates a condition is rejected as
            // `SignatureDoesNotMatch`, not `AccessDenied`.
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
                if !fakecloud_aws::sigv4::verify_signature(
                    &resolved.secret_access_key,
                    &credential.date_stamp,
                    &credential.region,
                    &credential.service,
                    &policy_b64,
                    signature,
                ) {
                    return Err(signature_does_not_match(
                    "The request signature we calculated does not match the signature you provided",
                ));
                }
            }

            // --- Policy validation (expiration + conditions) ---
            // Real S3 requires the policy to carry an `expiration`; a policy
            // without one is rejected rather than treated as never-expiring.
            let expiration = policy
                .get("expiration")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    invalid_argument("Invalid Policy: policy document must contain an `expiration`")
                })?;
            let expires_at = chrono::DateTime::parse_from_rfc3339(expiration)
                .map_err(|_| invalid_argument("policy `expiration` is not valid RFC3339"))?
                .with_timezone(&Utc);
            if Utc::now() > expires_at {
                return Err(access_denied(
                    "Invalid according to Policy: Policy expired.",
                ));
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
            // Every submitted field must be authorized by a policy condition.
            enforce_no_extra_fields(&conditions, &form)?;
            Some(credential.access_key)
        };

        // --- Store the object via the existing PutObject sink ---
        let mut synth_headers = HeaderMap::new();
        synth_headers.insert(
            http::header::CONTENT_TYPE,
            object_content_type
                .parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("binary/octet-stream")),
        );
        // The form makes a PutObject: its fields that are PutObject's request
        // headers keep their names (`PUT_OBJECT_FORM_HEADERS`, plus user
        // metadata `x-amz-meta-*`); `acl` is the canned ACL (`x-amz-acl`) and
        // `tagging` the object's tag set as a `Tagging` XML document (POST
        // Object API reference), which PutObject takes as the URL-encoded
        // `x-amz-tagging`. Any other field (the signature material, a copy
        // source or directive, which PutObject has no use for) is not a
        // header of it, though like every field it must be named by the
        // policy's conditions (checked above).
        for f in &form.fields {
            let lower = f.name.to_ascii_lowercase();
            if lower == "acl" {
                if let Ok(value) = f.value.parse::<http::HeaderValue>() {
                    synth_headers.insert("x-amz-acl", value);
                }
                continue;
            }
            if lower == "tagging" {
                let tags = tagging_document(&f.value)?;
                if let Ok(value) = tagging_header(&tags).parse::<http::HeaderValue>() {
                    synth_headers.insert("x-amz-tagging", value);
                }
                continue;
            }
            let forwardable = PUT_OBJECT_FORM_HEADERS.contains(&lower.as_str())
                || lower.starts_with("x-amz-meta-");
            if !forwardable {
                continue;
            }
            // Browser form fields are UTF-8. A non-ASCII metadata value is
            // carried as an RFC 2047 encoded-word so the PutObject sink
            // decodes it as UTF-8 rather than as raw ISO-8859-1 header bytes.
            let value = if lower.starts_with("x-amz-meta-") {
                fakecloud_core::rfc2047::encode(&f.value)
            } else {
                f.value.clone()
            };
            if let (Ok(name), Ok(value)) = (
                lower.parse::<http::header::HeaderName>(),
                value.parse::<http::HeaderValue>(),
            ) {
                synth_headers.insert(name, value);
            }
        }

        // Authorized as the PutObject it is, on the object the form names,
        // for the uploader the policy was signed by (or anonymously): AWS
        // authorizes a POST Object as s3:PutObject, plus s3:PutObjectAcl /
        // s3:PutObjectTagging / the Object Lock actions for the ACL, tagging
        // and lock fields it sets, exactly as PutObject's headers. The
        // evaluation sees that PutObject -- the form's fields as its headers
        // (so `s3:x-amz-server-side-encryption`, `s3:x-amz-acl`,
        // `aws:RequestTag/*` ... come from the form) and the connection's
        // own headers -- not the multipart POST that carried it. Dispatch
        // cannot see the key, the fields or the signer, so this is the
        // handler's (`iam_authorized_by_handler`).
        let mut authorized_headers = synth_headers.clone();
        for name in [
            "host",
            "origin",
            "referer",
            "user-agent",
            "x-forwarded-proto",
            "x-forwarded-for",
        ] {
            if let Some(value) = req.headers.get(name) {
                authorized_headers.insert(name, value.clone());
            }
        }
        let as_put = AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: req.region.clone(),
            account_id: req.account_id.clone(),
            request_id: req.request_id.clone(),
            headers: authorized_headers,
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![bucket.to_string(), resolved_key.clone()],
            raw_path: format!("/{bucket}/{resolved_key}"),
            raw_query: String::new(),
            method: Method::PUT,
            is_query_protocol: false,
            access_key_id: signer.clone(),
            principal: None,
        };
        // A PutObject by construction: only PutObject's own headers were
        // carried over, so no field can make it classify as anything else
        // (a copy, say) and change its condition keys or request tags.
        debug_assert_eq!(
            crate::service::s3_operation(&as_put),
            Some("PutObject"),
            "a form upload is evaluated as a PutObject"
        );
        let mut actions = vec!["PutObject"];
        actions.extend(
            crate::service::iam_actions::object_write_extras(&synth_headers)
                .into_iter()
                .filter_map(|need| match need {
                    crate::service::iam_actions::Need::Own(_, action) => Some(action),
                    _ => None,
                }),
        );
        let allowed = actions
            .into_iter()
            .all(|action| self.caller_may_for(&as_put, action, bucket, &resolved_key));
        if !allowed {
            return Err(access_denied("Access Denied"));
        }

        let synth_req = AwsRequest {
            service: "s3".to_string(),
            // PutObject's handler reads the action to decide which
            // ObjectCreated event to fire; a browser form upload is
            // ObjectCreated:Post on real S3.
            action: "PostObject".to_string(),
            region: req.region.clone(),
            // The caller, not the bucket owner `account_id` the write is
            // served in: the object-created event reports this requester.
            account_id: req.account_id.clone(),
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
        let mut headers = HeaderMap::new();
        headers.insert(
            "etag",
            etag.parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("\"\"")),
        );

        // `success_action_redirect` (legacy alias `redirect`) takes precedence
        // over `success_action_status`: on success S3 issues a 303 to the given
        // URL with `bucket`, `key`, and `etag` appended as query parameters.
        if let Some(redirect) = form
            .field_lower("success_action_redirect")
            .or_else(|| form.field_lower("redirect"))
            .filter(|s| !s.is_empty())
        {
            // Query-component encoding: percent-encode everything unsafe but
            // leave the RFC 3986 unreserved characters (`-._~`) intact, the way
            // real S3 encodes the redirect query rather than over-escaping.
            const QUERY_ENC: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
                .remove(b'-')
                .remove(b'.')
                .remove(b'_')
                .remove(b'~');
            let enc = |s: &str| percent_encoding::utf8_percent_encode(s, QUERY_ENC).to_string();
            let sep = if redirect.contains('?') { '&' } else { '?' };
            let target = format!(
                "{redirect}{sep}bucket={}&key={}&etag={}",
                enc(bucket),
                enc(&resolved_key),
                enc(etag.trim_matches('"')),
            );
            if let Ok(value) = target.parse() {
                headers.insert("location", value);
                return Ok(AwsResponse {
                    status: StatusCode::SEE_OTHER,
                    content_type: String::new(),
                    body: Bytes::new().into(),
                    headers,
                });
            }
        }

        let success_status = form
            .field_lower("success_action_status")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(204);
        let location = object_url(req, bucket, &resolved_key);

        match success_status {
            200 => Ok(AwsResponse {
                status: StatusCode::OK,
                content_type: String::new(),
                body: Bytes::new().into(),
                headers,
            }),
            201 => {
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

#[cfg(test)]
mod form_tests {
    use super::*;

    /// The headers a form field may carry are exactly PutObject's own
    /// (`PutObjectRequest`'s `@httpHeader` members), less the three it sets
    /// otherwise: never a copy source, a directive or the signature.
    #[test]
    fn form_headers_are_put_objects_request_headers() {
        let model: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../aws-models/s3.json"
            ))
            .unwrap(),
        )
        .unwrap();
        let members = model["shapes"]["com.amazonaws.s3#PutObjectRequest"]["members"]
            .as_object()
            .unwrap();
        let mut headers: Vec<String> = members
            .values()
            .filter_map(|m| m["traits"]["smithy.api#httpHeader"].as_str())
            .map(str::to_ascii_lowercase)
            .filter(|h| {
                !matches!(
                    h.as_str(),
                    "content-type" | "content-length" | "x-amz-acl" | "x-amz-tagging"
                )
            })
            .collect();
        headers.sort();
        let mut ours: Vec<String> = PUT_OBJECT_FORM_HEADERS
            .iter()
            .map(|h| h.to_string())
            .collect();
        ours.sort();
        assert_eq!(ours, headers);
        assert!(!ours.iter().any(|h| h.starts_with("x-amz-copy-source")
            || h.ends_with("-directive")
            || matches!(
                h.as_str(),
                "x-amz-credential" | "x-amz-signature" | "x-amz-date"
            )));
    }

    #[test]
    fn tagging_fields_are_validated_documents() {
        let tag = |k: &str, v: &str| format!("<Tag><Key>{k}</Key><Value>{v}</Value></Tag>");
        let doc = |tags: &str| format!("<Tagging><TagSet>{tags}</TagSet></Tagging>");
        assert_eq!(
            tagging_document(&doc(&tag("team", "a"))).unwrap(),
            vec![("team".to_string(), "a".to_string())]
        );
        for malformed in [
            "not xml".to_string(),
            "<Tagging><TagSet>".to_string(),
            "<Other><TagSet></TagSet></Other>".to_string(),
            doc("<Tag><Value>v</Value></Tag>"),
            doc("<Tag/>"),
        ] {
            assert_eq!(
                tagging_document(&malformed).unwrap_err().code(),
                "MalformedXML",
                "{malformed}"
            );
        }
        let duplicate = doc(&format!("{}{}", tag("k", "1"), tag("k", "2")));
        assert_eq!(
            tagging_document(&duplicate).unwrap_err().code(),
            "InvalidTag"
        );
        let system = doc(&tag("aws:x", "1"));
        assert_eq!(tagging_document(&system).unwrap_err().code(), "InvalidTag");
        let eleven: String = (0..11).map(|i| tag(&format!("k{i}"), "v")).collect();
        assert_eq!(
            tagging_document(&doc(&eleven)).unwrap_err().code(),
            "BadRequest"
        );
        let ten: String = (0..10).map(|i| tag(&format!("k{i}"), "v")).collect();
        assert_eq!(tagging_document(&doc(&ten)).unwrap().len(), 10);
    }
}
