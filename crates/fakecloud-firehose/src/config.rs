use std::time::Duration;

use fakecloud_core::service::AwsServiceError;
use fakecloud_s3::delivery::DeliveredEncryption;
use http::StatusCode;
use serde_json::Value;

use crate::output::{append_delimiter_enabled, delivery_encryption, OutputConfig};
use crate::state::S3Destination;

#[derive(Clone, Debug)]
pub struct BufferingPolicy {
    pub size_bytes: usize,
    pub interval: Duration,
}

#[derive(Clone, Debug)]
pub struct DeliveryConfig {
    pub bucket: String,
    pub output: OutputConfig,
    pub buffering: BufferingPolicy,
    /// Server-side encryption of every delivered object: SSE-KMS under the
    /// destination's `KMSEncryptionConfig` key, otherwise the bucket default.
    /// Captured with the rest of the configuration, so a batch keeps the key
    /// it was admitted under through retries.
    pub encryption: DeliveredEncryption,
    /// Set for Elasticsearch / OpenSearch Service destinations, whose S3
    /// fields describe the backup bucket.
    pub search: Option<crate::search::SearchDelivery>,
}

pub(crate) fn invalid_argument(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", message)
}

/// Validate the paired API hints and materialize Firehose's S3 defaults.
pub fn buffering_policy(
    size: Option<i64>,
    interval: Option<i64>,
) -> Result<BufferingPolicy, AwsServiceError> {
    let (size, interval) = match (size, interval) {
        (None, None) => (5, 300),
        (Some(size), Some(interval)) => (size, interval),
        _ => {
            return Err(invalid_argument(
                "BufferingHints must specify both SizeInMBs and IntervalInSeconds",
            ))
        }
    };
    if !(1..=128).contains(&size) {
        return Err(invalid_argument(format!(
            "BufferingHints.SizeInMBs must be between 1 and 128, got {size}"
        )));
    }
    if !(0..=900).contains(&interval) {
        return Err(invalid_argument(format!(
            "BufferingHints.IntervalInSeconds must be between 0 and 900, got {interval}"
        )));
    }
    Ok(BufferingPolicy {
        size_bytes: size as usize * 1_048_576,
        interval: Duration::from_secs(interval as u64),
    })
}

/// Read supplied integers strictly: a malformed member is not an omitted hint.
pub(crate) fn parse_buffering_hints(
    value: &Value,
) -> Result<(Option<i64>, Option<i64>), AwsServiceError> {
    let Some(hints) = value.get("BufferingHints") else {
        return Ok((None, None));
    };
    if !hints.is_object() {
        return Err(invalid_argument("BufferingHints must be an object"));
    }
    let integer = |field: &str| {
        hints
            .get(field)
            .map(|value| {
                value.as_i64().ok_or_else(|| {
                    invalid_argument(format!("BufferingHints.{field} must be an integer"))
                })
            })
            .transpose()
    };
    let size = integer("SizeInMBs")?;
    let interval = integer("IntervalInSeconds")?;
    buffering_policy(size, interval)?;
    Ok((size, interval))
}

pub(crate) fn validate_file_extension(value: &Value) -> Result<(), AwsServiceError> {
    let Some(extension) = value.get("FileExtension") else {
        return Ok(());
    };
    let extension = extension
        .as_str()
        .ok_or_else(|| invalid_argument("FileExtension must be a string"))?;
    // Equivalent to ^(|\.[0-9a-z!\-_.*'()]+)$ without a regex dependency.
    if extension.len() > 128
        || (!extension.is_empty()
            && (!extension.starts_with('.')
                || extension.len() < 2
                || !extension[1..].bytes().all(|c| {
                    c.is_ascii_digit()
                        || c.is_ascii_lowercase()
                        || matches!(c, b'!' | b'-' | b'_' | b'.' | b'*' | b'\'' | b'(' | b')')
                })))
    {
        return Err(invalid_argument("FileExtension must be empty or a dot followed by lowercase letters, digits, or !-_.*'(), with at most 128 characters"));
    }
    Ok(())
}

/// Enforce the Firehose model's length limits on S3 destination strings.
/// Every buffered batch and object key copies these values, so unbounded input
/// would multiply memory use across retained batches.
pub(crate) fn validate_destination_strings(value: &Value) -> Result<(), AwsServiceError> {
    for (field, min, max) in [
        ("RoleARN", 1, 512),
        ("BucketARN", 1, 2048),
        ("Prefix", 0, 1024),
        ("ErrorOutputPrefix", 0, 1024),
        ("CustomTimeZone", 0, 50),
    ] {
        let Some(member) = value.get(field).and_then(Value::as_str) else {
            continue;
        };
        // Smithy lengths count Unicode scalar values, not bytes.
        let length = member.chars().count();
        if !(min..=max).contains(&length) {
            return Err(invalid_argument(format!(
                "{field} must be between {min} and {max} characters, got {length}"
            )));
        }
    }
    Ok(())
}

/// Shared control-plane parser. Unsupported delivery features still round-trip;
/// record admission uses `delivery_config` separately.
pub fn parse_s3_destination(value: &Value) -> Result<Option<S3Destination>, AwsServiceError> {
    crate::service::parse_s3_destination(value)
}

pub(crate) fn s3_bucket_name(dest: &S3Destination) -> Result<&str, AwsServiceError> {
    fakecloud_aws::arn::arn_resource(&dest.bucket_arn, "s3")
        .and_then(|resource| resource.strip_prefix("::"))
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| invalid_argument("S3 destination requires a bucket ARN"))
}

/// Capture an owned, immutable delivery configuration before record admission.
/// This helper does not restrict configuration create/update/describe acceptance.
pub fn delivery_config(dest: &S3Destination) -> Result<DeliveryConfig, AwsServiceError> {
    let compression = crate::admission::validate_s3_delivery(dest)?;
    let prefix = dest.prefix.as_deref().unwrap_or_default();
    let bucket = s3_bucket_name(dest)?;
    // Older snapshots can omit either hint; fill defaults independently here.
    let buffering = buffering_policy(
        Some(dest.buffering_size_mb.unwrap_or(5)),
        Some(dest.buffering_interval_seconds.unwrap_or(300)),
    )?;
    Ok(DeliveryConfig {
        bucket: bucket.to_owned(),
        output: OutputConfig {
            prefix: prefix.to_owned(),
            file_extension: dest.file_extension.clone(),
            compression,
            append_delimiter: append_delimiter_enabled(dest.processing_configuration.as_ref()),
        },
        buffering,
        encryption: delivery_encryption(dest.encryption_configuration.as_ref()),
        search: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::Compression;
    use http::StatusCode;
    use serde_json::json;
    use std::time::Duration;

    fn destination(extra: serde_json::Value) -> crate::S3Destination {
        let mut value = json!({"RoleARN": "arn:aws:iam::123456789012:role/fh", "BucketARN": "arn:aws:s3:::bucket"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        parse_s3_destination(&value).unwrap().unwrap()
    }

    fn unsupported(dest: &crate::S3Destination) {
        let error = delivery_config(dest).unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "InvalidArgumentException");
    }

    #[test]
    fn unsupported_admission_names_setting_with_invalid_argument() {
        for (extra, setting) in [
            (json!({"S3BackupMode": "Enabled"}), "S3BackupMode"),
            (json!({"CompressionFormat": "ZIP"}), "CompressionFormat"),
            (
                json!({"CompressionFormat": "HADOOP_SNAPPY"}),
                "HADOOP_SNAPPY",
            ),
            (json!({"Prefix": "!{timestamp:yyyy}/"}), "Prefix"),
            (
                json!({"CustomTimeZone": "America/New_York"}),
                "CustomTimeZone",
            ),
            (
                json!({"ProcessingConfiguration": {"Enabled": true}}),
                "ProcessingConfiguration",
            ),
            (
                json!({"DataFormatConversionConfiguration": {"Enabled": true}}),
                "DataFormatConversionConfiguration",
            ),
            (
                json!({"DynamicPartitioningConfiguration": {"Enabled": true}}),
                "DynamicPartitioningConfiguration",
            ),
        ] {
            // Control-plane acceptance remains separate from admission.
            let dest = destination(extra);
            let error = delivery_config(&dest).unwrap_err();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "InvalidArgumentException");
            assert!(error.message().contains(setting), "{}", error.message());
        }
    }

    #[test]
    fn destination_strings_follow_model_limits() {
        let base = || json!({"RoleARN": "arn:aws:iam::123456789012:role/fh", "BucketARN": "arn:aws:s3:::bucket"});
        // Limits count characters: 1,024 multi-byte characters still fit.
        for (field, accepted) in [
            ("Prefix", "é".repeat(1024)),
            ("ErrorOutputPrefix", "e".repeat(1024)),
            ("Prefix", String::new()),
            (
                "CustomTimeZone",
                "America/Argentina/Buenos_Aires".to_owned(),
            ),
            ("CustomTimeZone", String::new()),
            ("RoleARN", "r".repeat(512)),
            ("BucketARN", format!("arn:aws:s3:::{}", "b".repeat(2035))),
        ] {
            let mut value = base();
            value[field] = json!(accepted);
            parse_s3_destination(&value).unwrap().unwrap();
        }
        for (field, rejected) in [
            ("Prefix", "a".repeat(1025)),
            ("ErrorOutputPrefix", "a".repeat(1025)),
            ("CustomTimeZone", "Z".repeat(51)),
            ("RoleARN", String::new()),
            ("RoleARN", "r".repeat(513)),
            ("BucketARN", String::new()),
            ("BucketARN", format!("arn:aws:s3:::{}", "b".repeat(2036))),
        ] {
            let mut value = base();
            value[field] = json!(rejected);
            let error = parse_s3_destination(&value).unwrap_err();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "InvalidArgumentException");
            assert!(error.message().contains(field), "{}", error.message());
        }
    }

    /// Destination KMS is delivered (as SSE-KMS), not rejected at admission,
    /// and the key is captured with the rest of the delivery configuration.
    #[test]
    fn kms_encryption_is_captured_not_rejected() {
        let arn = "arn:aws:kms:us-east-1:123456789012:key/example";
        let dest = destination(json!({
            "EncryptionConfiguration": {"KMSEncryptionConfig": {"AWSKMSKeyARN": arn}}
        }));
        let config = delivery_config(&dest).unwrap();
        assert_eq!(
            config.encryption,
            DeliveredEncryption::SseKms {
                key_id: Some(arn.to_owned())
            }
        );
        for extra in [
            json!({}),
            json!({"EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"}}),
        ] {
            assert_eq!(
                delivery_config(&destination(extra)).unwrap().encryption,
                DeliveredEncryption::AsDelivered
            );
        }
    }

    #[test]
    fn default_buffering() {
        let policy = buffering_policy(None, None).unwrap();
        assert_eq!(policy.size_bytes, 5 * 1_048_576);
        assert_eq!(policy.interval, Duration::from_secs(300));
        let dest = destination(json!({}));
        assert_eq!(dest.buffering_size_mb, Some(5));
        assert_eq!(dest.buffering_interval_seconds, Some(300));
        let mut legacy = dest;
        legacy.buffering_size_mb = None;
        legacy.buffering_interval_seconds = None;
        let config = delivery_config(&legacy).unwrap();
        assert_eq!(config.buffering.size_bytes, 5 * 1_048_576);
        assert_eq!(config.buffering.interval, Duration::from_secs(300));
        legacy.buffering_size_mb = Some(1);
        assert_eq!(
            delivery_config(&legacy).unwrap().buffering.size_bytes,
            1_048_576
        );
    }

    #[test]
    fn paired_hints_and_interval_boundaries() {
        for (size, interval) in [(1, 0), (1, 1), (128, 900)] {
            let policy = buffering_policy(Some(size), Some(interval)).unwrap();
            assert_eq!(policy.size_bytes, size as usize * 1_048_576);
            assert_eq!(policy.interval, Duration::from_secs(interval as u64));
        }
        for (size, interval) in [
            (Some(0), Some(300)),
            (Some(129), Some(300)),
            (Some(1), Some(-1)),
            (Some(1), Some(901)),
            (Some(1), None),
            (None, Some(300)),
        ] {
            let error = buffering_policy(size, interval).unwrap_err();
            assert_eq!(error.code(), "InvalidArgumentException");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn typed_integer_hints() {
        for bad in [
            json!("5"),
            json!(1.5),
            json!(true),
            json!(null),
            json!(u64::MAX),
            json!({}),
        ] {
            for field in ["SizeInMBs", "IntervalInSeconds"] {
                let mut value = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300}});
                value["BufferingHints"][field] = bad.clone();
                assert_eq!(
                    parse_s3_destination(&value).unwrap_err().code(),
                    "InvalidArgumentException",
                    "{value}"
                );
            }
        }
        for hints in [
            json!(null),
            json!("hints"),
            json!([]),
            json!({"SizeInMBs": 5}),
            json!({"IntervalInSeconds": 300}),
        ] {
            let value = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "BufferingHints": hints});
            assert!(parse_s3_destination(&value).is_err(), "{value}");
        }
    }

    #[test]
    fn delivery_preflight() {
        for (codec, expected) in [
            ("UNCOMPRESSED", Compression::Uncompressed),
            ("GZIP", Compression::Gzip),
            ("Snappy", Compression::Snappy),
        ] {
            for zone in [None, Some(""), Some("UTC")] {
                for prefix in [None, Some("static/")] {
                    let mut dest = destination(json!({"CompressionFormat": codec}));
                    dest.custom_time_zone = zone.map(str::to_owned);
                    dest.prefix = prefix.map(str::to_owned);
                    let config = delivery_config(&dest).unwrap();
                    assert_eq!(config.bucket, "bucket");
                    assert_eq!(config.output.compression, expected);
                    assert_eq!(config.output.prefix, prefix.unwrap_or_default());
                    let captured = config.clone();
                    dest.prefix = Some("changed/".into());
                    assert_eq!(captured.output.prefix, prefix.unwrap_or_default());
                    assert!(!format!("{captured:?}").is_empty());
                }
            }
        }
        assert_eq!(
            delivery_config(&destination(json!({})))
                .unwrap()
                .output
                .compression,
            Compression::Uncompressed
        );
        for codec in ["ZIP", "HADOOP_SNAPPY", "SNAPPY"] {
            unsupported(&destination(json!({"CompressionFormat": codec})));
        }
        unsupported(&destination(json!({"Prefix": "!{timestamp:yyyy}/"})));
        unsupported(&destination(json!({"CustomTimeZone": "America/New_York"})));
        for partition in ["aws", "aws-cn", "aws-us-gov"] {
            let dest = destination(json!({"BucketARN": format!("arn:{partition}:s3:::bucket")}));
            assert_eq!(delivery_config(&dest).unwrap().bucket, "bucket");
        }
        assert!(delivery_config(&destination(json!({"BucketARN": "not-an-arn"}))).is_err());
    }

    #[test]
    fn enabled_pipeline_preflight() {
        for extra in [
            json!({"ProcessingConfiguration": {"Enabled": true}}),
            json!({"DataFormatConversionConfiguration": {"Enabled": true}}),
            json!({"DataFormatConversionConfiguration": {"SchemaConfiguration": {}}}),
            json!({"DynamicPartitioningConfiguration": {"Enabled": true}}),
        ] {
            unsupported(&destination(extra));
        }
        let config = delivery_config(&destination(json!({
            "ProcessingConfiguration": {"Enabled": false, "Processors": [{"Type": "Lambda"}]},
            "DataFormatConversionConfiguration": {"Enabled": false},
            "DynamicPartitioningConfiguration": {"Enabled": false}
        })))
        .unwrap();
        assert_eq!(config.bucket, "bucket");
        assert!(!config.output.append_delimiter);
    }

    #[test]
    fn only_append_delimiter_processing_is_delivered() {
        let delimiter = json!({"Type": "AppendDelimiterToRecord"});
        let with_parameter = json!({"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]});
        for (processing, append_delimiter) in [
            (json!({"Enabled": true, "Processors": [delimiter]}), true),
            (
                json!({"Enabled": true, "Processors": [with_parameter]}),
                true,
            ),
            (
                json!({"Enabled": true, "Processors": [delimiter, delimiter]}),
                true,
            ),
            (
                json!({"Enabled": false, "Processors": [{"Type": "Lambda"}]}),
                false,
            ),
            (json!({"Processors": [delimiter]}), false),
            (
                json!({"Enabled": "true", "Processors": [{"Type": "Lambda"}]}),
                false,
            ),
        ] {
            let dest = destination(json!({"ProcessingConfiguration": processing}));
            assert_eq!(
                delivery_config(&dest).unwrap().output.append_delimiter,
                append_delimiter,
                "{processing}"
            );
        }
        for (processing, reason) in [
            (
                json!({"Enabled": true}),
                "without an AppendDelimiterToRecord",
            ),
            (
                json!({"Enabled": true, "Processors": []}),
                "without an AppendDelimiterToRecord",
            ),
            (
                json!({"Enabled": true, "Processors": {"Type": "AppendDelimiterToRecord"}}),
                "without an AppendDelimiterToRecord",
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
                "processor Lambda",
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}, delimiter]}),
                "processor Lambda",
            ),
            (
                json!({"Enabled": true, "Processors": [delimiter, {"Type": "RecordDeAggregation"}]}),
                "processor RecordDeAggregation",
            ),
            (
                json!({"Enabled": true, "Processors": [delimiter, {}]}),
                "processor untyped",
            ),
        ] {
            let dest = destination(json!({"ProcessingConfiguration": processing}));
            unsupported(&dest);
            let message = delivery_config(&dest).unwrap_err().message();
            assert!(message.contains("ProcessingConfiguration"), "{message}");
            assert!(message.contains(reason), "{processing}: {message}");
        }
    }

    #[test]
    fn file_extension_validation() {
        for extension in ["", ".log", ".a0!-_.*'()"] {
            let dest = destination(json!({"FileExtension": extension}));
            assert_eq!(
                delivery_config(&dest)
                    .unwrap()
                    .output
                    .file_extension
                    .as_deref(),
                Some(extension)
            );
        }
        let valid = format!(".{}", "a".repeat(127));
        assert_eq!(
            destination(json!({"FileExtension": valid}))
                .file_extension
                .as_deref(),
            Some(valid.as_str())
        );
        for extension in [
            "log".to_string(),
            ".".into(),
            ".UPPER".into(),
            ".é".into(),
            ".a/b".into(),
            ".a b".into(),
            format!(".{}", "a".repeat(128)),
        ] {
            let value = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "FileExtension": extension});
            assert_eq!(
                parse_s3_destination(&value).unwrap_err().code(),
                "InvalidArgumentException"
            );
        }
        assert!(parse_s3_destination(
            &json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "FileExtension": 1})
        )
        .is_err());
    }
}
