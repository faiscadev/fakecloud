//! `S3Service` `encryption` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Encryption ----

    pub(crate) fn put_bucket_encryption(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        // Validate the SSE config fields *before* persisting. The stored
        // body is later round-tripped, field-by-field, into the object
        // default-encryption headers on PutObject and ultimately into the
        // `x-amz-server-side-encryption` / `-aws-kms-key-id` *response*
        // headers on a later GET/HEAD. A control byte here (all bytes
        // 0x00-0x1F are legal XML body bytes but illegal HTTP header bytes)
        // would otherwise be persisted verbatim and crash the read path —
        // a reachable, unauthenticated-input DoS (bug-audit 2026-06-13 2.1/2.2).
        if let Some(algo) = extract_xml_value(&body_str, "SSEAlgorithm") {
            // SSEAlgorithm is a closed enum; AWS returns MalformedXML for
            // anything outside it. The full set is the Smithy
            // `ServerSideEncryption` enum in `aws-models/s3.json` — which
            // includes the managed-service algorithms `aws:fsx` and
            // `aws:backup` alongside the three customer-facing ones.
            if !matches!(
                algo.as_str(),
                "AES256" | "aws:kms" | "aws:kms:dsse" | "aws:fsx" | "aws:backup"
            ) {
                return Err(malformed_encryption("SSEAlgorithm", &algo));
            }
        }
        if let Some(kid) = extract_xml_value(&body_str, "KMSMasterKeyID") {
            // A real KMS key id / ARN / alias never contains control
            // characters. Reject any value that isn't a legal HTTP header
            // value rather than persisting a string that would later panic
            // the GET/HEAD response path. AWS rejects a malformed key id with
            // KMS.KMSInvalidStateException / InvalidArgument; InvalidArgument
            // is the closest stable shape here.
            if kid.parse::<http::header::HeaderValue>().is_err() {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "The KMSMasterKeyID is not a valid KMS key id, ARN, or alias",
                    vec![
                        ("ArgumentName".to_string(), "KMSMasterKeyID".to_string()),
                        ("ArgumentValue".to_string(), kid.clone()),
                    ],
                ));
            }
        }
        // aws:kms / aws:kms:dsse rules require KMSMasterKeyID — AWS
        // rejects these with InvalidArgument when the field is
        // missing or empty, since the bucket would otherwise have
        // no key to encrypt against.
        if (body_str.contains("aws:kms") || body_str.contains("aws:kms:dsse"))
            && !has_kms_master_key_id(&body_str)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Default KMS encryption requires KMSMasterKeyID",
            ));
        }
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Normalize: add BucketKeyEnabled=false to each <Rule> that
        // is missing one. The prior whole-body check skipped this when
        // a different rule already had BucketKeyEnabled, so mixed
        // configs would drop the default on rules that needed it.
        let normalized = {
            let mut out = String::with_capacity(body_str.len() + 64);
            let mut cursor = 0usize;
            while let Some(start) = body_str[cursor..].find("<Rule>") {
                let abs_start = cursor + start;
                out.push_str(&body_str[cursor..abs_start]);
                let rule_open = abs_start;
                let rest = &body_str[rule_open..];
                if let Some(end_rel) = rest.find("</Rule>") {
                    let rule_block = &rest[..end_rel];
                    let mut rule_out = rule_block.to_string();
                    if !rule_block.contains("<BucketKeyEnabled>") {
                        rule_out.push_str("<BucketKeyEnabled>false</BucketKeyEnabled>");
                    }
                    rule_out.push_str("</Rule>");
                    out.push_str(&rule_out);
                    cursor = rule_open + end_rel + "</Rule>".len();
                } else {
                    out.push_str(rest);
                    cursor = body_str.len();
                    break;
                }
            }
            out.push_str(&body_str[cursor..]);
            out
        };
        b.encryption_config = Some(normalized.clone());
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Encryption, &normalized)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_encryption(
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
        match &b.encryption_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    pub(crate) fn delete_bucket_encryption(
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
        b.encryption_config = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Encryption)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

/// Build a `MalformedXML` error for a bucket-encryption field whose value
/// isn't a member of its closed enum (e.g. an `SSEAlgorithm` other than
/// AES256/aws:kms/aws:kms:dsse). Matches AWS, which rejects such bodies with
/// 400 MalformedXML.
fn malformed_encryption(field: &str, value: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against \
         our published schema",
        vec![
            ("ArgumentName".to_string(), field.to_string()),
            ("ArgumentValue".to_string(), value.to_string()),
        ],
    )
}
