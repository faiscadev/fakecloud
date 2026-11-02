//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `firehose`.

use super::*;

/// CFN currently provisions the classic and Extended S3 configurations. Check
/// their enum inputs, including the backup blocks, with the same validator the
/// Firehose API uses before parsing or locking state; the CFN parser otherwise
/// silently discards non-string enum values.
fn validate_firehose_compression(props: &serde_json::Value) -> Result<(), String> {
    for member in [
        "S3DestinationConfiguration",
        "ExtendedS3DestinationConfiguration",
    ] {
        fakecloud_firehose::validate_s3_destination_compression(&props[member], false)
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// Members of the Firehose S3 destination shapes (and every block nested in
/// them: backup configuration, buffering hints, logging options, dynamic
/// partitioning retry options, format conversion SerDes) that the Firehose
/// Smithy model types as `boolean`. No member of these names is a string.
const FIREHOSE_BOOLEAN_MEMBERS: &[&str] = &[
    "CaseInsensitive",
    "ConvertDotsInJsonKeysToUnderscores",
    "EnableDictionaryCompression",
    "EnablePadding",
    "Enabled",
];

/// The same shapes' `integer` members.
const FIREHOSE_INTEGER_MEMBERS: &[&str] = &[
    "BlockSizeBytes",
    "DurationInSeconds",
    "IntervalInSeconds",
    "MaxPaddingBytes",
    "PageSizeBytes",
    "RowIndexStride",
    "SizeInMBs",
    "StripeSizeBytes",
];

/// The same shapes' `double` members.
const FIREHOSE_DOUBLE_MEMBERS: &[&str] = &[
    "BloomFilterFalsePositiveProbability",
    "DictionaryKeyThreshold",
    "PaddingTolerance",
];

/// Map members whose keys are user data, not model member names.
const FIREHOSE_FREE_FORM_MAPS: &[&str] = &["ColumnToJsonKeyMappings"];

/// Give the modeled boolean and numeric members of a Firehose S3 destination
/// block their API types. CloudFormation passes template parameters (`Ref`)
/// as strings, so `"false"` or `"60"` would otherwise be stored and echoed
/// by `DescribeDeliveryStream` where SDKs expect a boolean or a number. A
/// modeled member whose string does not parse exactly (no surrounding
/// whitespace; integers within the model's 32-bit range), or whose JSON value
/// has the wrong type, fails the resource, as CloudFormation's schema
/// validation does, rather than being stored as a value no SDK can
/// deserialize. `property` (the top-level property name) prefixes the
/// member path in the error.
pub(super) fn coerce_firehose_scalars(
    mut value: serde_json::Value,
    property: &str,
) -> Result<serde_json::Value, String> {
    coerce_firehose_scalars_in_place(&mut value, property)?;
    Ok(value)
}

fn member_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}.{name}")
    }
}

#[derive(Clone, Copy)]
enum ScalarType {
    Boolean,
    Integer,
    Double,
}

impl ScalarType {
    fn of(member: &str) -> Option<Self> {
        if FIREHOSE_BOOLEAN_MEMBERS.contains(&member) {
            Some(Self::Boolean)
        } else if FIREHOSE_INTEGER_MEMBERS.contains(&member) {
            Some(Self::Integer)
        } else if FIREHOSE_DOUBLE_MEMBERS.contains(&member) {
            Some(Self::Double)
        } else {
            None
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Self::Boolean => "a boolean",
            Self::Integer => "a 32-bit integer",
            Self::Double => "a number",
        }
    }

    /// The value with its API type, or `None` if it is neither that type nor
    /// a string that parses as it.
    fn coerce(self, value: &serde_json::Value) -> Option<serde_json::Value> {
        use serde_json::Value;
        match (self, value) {
            (Self::Boolean, Value::Bool(_)) => Some(value.clone()),
            (Self::Boolean, Value::String(text)) => {
                if text.eq_ignore_ascii_case("true") {
                    Some(Value::Bool(true))
                } else if text.eq_ignore_ascii_case("false") {
                    Some(Value::Bool(false))
                } else {
                    None
                }
            }
            (Self::Integer, Value::Number(number)) => {
                let integer = match number.as_i64() {
                    Some(integer) => integer,
                    // An integral float (`60.0`) is still an integer.
                    None => {
                        let float = number.as_f64()?;
                        if float.fract() != 0.0 {
                            return None;
                        }
                        // Saturates out of range; the i32 check below rejects it.
                        float as i64
                    }
                };
                i32::try_from(integer).ok().map(Value::from)
            }
            (Self::Integer, Value::String(text)) => text.parse::<i32>().ok().map(Value::from),
            (Self::Double, Value::Number(_)) => Some(value.clone()),
            (Self::Double, Value::String(text)) => text
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number),
            _ => None,
        }
    }
}

fn coerce_firehose_scalars_in_place(
    value: &mut serde_json::Value,
    path: &str,
) -> Result<(), String> {
    match value {
        serde_json::Value::Object(members) => {
            for (name, member) in members.iter_mut() {
                let name = name.as_str();
                if FIREHOSE_FREE_FORM_MAPS.contains(&name) {
                    continue;
                }
                match ScalarType::of(name) {
                    // An explicit null is treated as an absent member.
                    Some(_) if member.is_null() => {}
                    Some(scalar) => match scalar.coerce(member) {
                        Some(coerced) => *member = coerced,
                        None => {
                            return Err(format!(
                                "{} must be {}, got {member}",
                                member_path(path, name),
                                scalar.expected()
                            ))
                        }
                    },
                    None => coerce_firehose_scalars_in_place(member, &member_path(path, name))?,
                }
            }
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                coerce_firehose_scalars_in_place(item, &format!("{path}[{index}]"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl ResourceProvisioner {
    // --- Firehose ---

    pub(super) fn create_firehose_delivery_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        validate_firehose_compression(props)?;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();

        let arn = self.regional_arn("firehose", &format!("deliverystream/{}", name));
        let stream_type = props
            .get("DeliveryStreamType")
            .and_then(|v| v.as_str())
            .unwrap_or("DirectPut")
            .to_string();

        let has_s3 = props.get("S3DestinationConfiguration").is_some();
        let has_extended_s3 = props.get("ExtendedS3DestinationConfiguration").is_some();
        if has_s3 && has_extended_s3 {
            return Err("Only one of S3DestinationConfiguration or ExtendedS3DestinationConfiguration may be set".to_string());
        }
        let destination = Some(if let Some(s3) = props.get("S3DestinationConfiguration") {
            parse_firehose_s3_destination(s3, "S3DestinationConfiguration")?
        } else if let Some(s3) = props.get("ExtendedS3DestinationConfiguration") {
            parse_firehose_s3_destination(s3, "ExtendedS3DestinationConfiguration")?
        } else {
            return Err("Delivery stream requires a destination configuration".to_string());
        });

        let mut tags = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for tag in arr {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        let stream = DeliveryStream {
            instance_id: uuid::Uuid::new_v4(),
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type: stream_type.clone(),
            created_at: Utc::now(),
            last_update: Utc::now(),
            version_id: "1".to_string(),
            destination,
            tags,
            encryption: None,
            extra_destinations: std::collections::BTreeMap::new(),
            kinesis_source: None,
        };

        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account
            .streams_mut(&self.region)
            .insert(name.clone(), stream);

        let mut attributes = BTreeMap::new();
        attributes.insert("Arn".to_string(), arn.clone());
        attributes.insert("DeliveryStreamName".to_string(), name.clone());

        Ok(ProvisionResult {
            physical_id: name,
            attributes,
        })
    }

    /// In-place `UpdateStack` for an `AWS::KinesisFirehose::DeliveryStream`.
    /// Mutates the stored `DeliveryStream` in place instead of the reprovision
    /// fallback's delete+recreate. Applies the mutable destination config + tags
    /// (matching `UpdateDestination`/tag ops), bumps the `version_id`, and
    /// preserves the identity (`arn`, `created_at`, `stream_type`) and any
    /// `extra_destinations` on the record.
    pub(super) fn update_firehose_delivery_stream(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        validate_firehose_compression(props)?;
        let name = existing.physical_id.clone();

        let has_s3 = props.get("S3DestinationConfiguration").is_some();
        let has_extended_s3 = props.get("ExtendedS3DestinationConfiguration").is_some();
        if has_s3 && has_extended_s3 {
            return Err("Only one of S3DestinationConfiguration or ExtendedS3DestinationConfiguration may be set".to_string());
        }
        let new_destination = if let Some(s3) = props.get("S3DestinationConfiguration") {
            Some(parse_firehose_s3_destination(
                s3,
                "S3DestinationConfiguration",
            )?)
        } else if let Some(s3) = props.get("ExtendedS3DestinationConfiguration") {
            Some(parse_firehose_s3_destination(
                s3,
                "ExtendedS3DestinationConfiguration",
            )?)
        } else {
            None
        };

        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        let stream = account
            .streams_mut(&self.region)
            .get_mut(&name)
            .ok_or_else(|| format!("Delivery stream {name} not yet provisioned"))?;

        if let Some(mut dest) = new_destination {
            if let Some(existing) = &stream.destination {
                dest.destination_id = existing.destination_id.clone();
            }
            stream.destination = Some(dest);
        }
        if props.get("Tags").is_some() {
            let mut tags = BTreeMap::new();
            if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
                for tag in arr {
                    if let (Some(k), Some(v)) = (
                        tag.get("Key").and_then(|v| v.as_str()),
                        tag.get("Value").and_then(|v| v.as_str()),
                    ) {
                        tags.insert(k.to_string(), v.to_string());
                    }
                }
            }
            stream.tags = tags;
        }
        stream.last_update = Utc::now();
        let next_version = stream.version_id.parse::<u64>().unwrap_or(1) + 1;
        stream.version_id = next_version.to_string();
        let arn = stream.arn.clone();

        let mut attributes = BTreeMap::new();
        attributes.insert("Arn".to_string(), arn);
        attributes.insert("DeliveryStreamName".to_string(), name.clone());
        Ok(ProvisionResult {
            physical_id: name,
            attributes,
        })
    }

    pub(super) fn delete_firehose_delivery_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account.streams_mut(&self.region).remove(physical_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{make_provisioner, make_resource};
    use super::*;
    use serde_json::{json, Value};

    fn configuration() -> Value {
        json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws-cn:s3:::bucket",
            "Prefix": "static/",
            "ErrorOutputPrefix": "errors/",
            "CompressionFormat": "ZIP",
            "ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "Lambda"}]},
            "DataFormatConversionConfiguration": {"Enabled": true, "SchemaConfiguration": {"DatabaseName": "db"}},
            "CloudWatchLoggingOptions": {"Enabled": true, "LogGroupName": "group"},
            "CustomTimeZone": "America/New_York",
            "S3BackupMode": "Enabled",
            "FileExtension": ".data",
            "DynamicPartitioningConfiguration": {"Enabled": true, "RetryOptions": {"DurationInSeconds": 60}},
            "EncryptionConfiguration": {"KMSEncryptionConfig": {"AWSKMSKeyARN": "key"}},
            "S3BackupConfiguration": {"RoleARN": "backup-role", "BucketARN": "arn:aws:s3:::backup"}
        })
    }

    fn delivery_stream(extended_s3: Value) -> ResourceDefinition {
        make_resource(
            "AWS::KinesisFirehose::DeliveryStream",
            "Firehose",
            json!({
                "DeliveryStreamName": "events",
                "ExtendedS3DestinationConfiguration": extended_s3,
            }),
        )
    }

    fn stored(prov: &ResourceProvisioner) -> DeliveryStream {
        prov.firehose_state
            .read()
            .get("123456789012")
            .unwrap()
            .streams("us-east-1")
            .unwrap()["events"]
            .clone()
    }

    #[test]
    fn firehose_shared_parser_retains_extended_fields_and_defaults() {
        let config = configuration();
        let dest =
            parse_firehose_s3_destination(&config, "ExtendedS3DestinationConfiguration").unwrap();
        assert_eq!(dest.buffering_size_mb, Some(5));
        assert_eq!(dest.buffering_interval_seconds, Some(300));
        assert_eq!(dest.prefix.as_deref(), Some("static/"));
        assert_eq!(dest.error_output_prefix.as_deref(), Some("errors/"));
        assert_eq!(dest.compression_format.as_deref(), Some("ZIP"));
        assert_eq!(dest.custom_time_zone.as_deref(), Some("America/New_York"));
        assert_eq!(dest.s3_backup_mode.as_deref(), Some("Enabled"));
        assert_eq!(dest.file_extension.as_deref(), Some(".data"));
        for (stored, field) in [
            (dest.processing_configuration, "ProcessingConfiguration"),
            (
                dest.data_format_conversion_configuration,
                "DataFormatConversionConfiguration",
            ),
            (dest.cloudwatch_logging_options, "CloudWatchLoggingOptions"),
            (
                dest.dynamic_partitioning_configuration,
                "DynamicPartitioningConfiguration",
            ),
            (dest.encryption_configuration, "EncryptionConfiguration"),
            (dest.s3_backup_description, "S3BackupConfiguration"),
        ] {
            assert_eq!(stored.as_ref(), Some(&config[field]), "{field}");
        }
        for bad in [
            json!({"BufferingHints": {"SizeInMBs": 1}}),
            json!({"BufferingHints": {"SizeInMBs": "five", "IntervalInSeconds": 300}}),
            json!({"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 901}}),
            json!({"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": "901"}}),
            json!({"FileExtension": "invalid"}),
        ] {
            let mut value = config.clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(bad.as_object().unwrap().clone());
            assert!(
                parse_firehose_s3_destination(&value, "ExtendedS3DestinationConfiguration")
                    .is_err(),
                "{value}"
            );
        }
        assert!(parse_firehose_s3_destination(
            &json!({"BucketARN": "arn:aws:s3:::bucket"}),
            "ExtendedS3DestinationConfiguration"
        )
        .is_err());
        assert!(parse_firehose_s3_destination(
            &json!({"RoleARN": "role"}),
            "ExtendedS3DestinationConfiguration"
        )
        .is_err());
    }

    #[test]
    fn firehose_create_update_retains_configuration_and_stable_identity() {
        for shape in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            let prov = make_provisioner();
            let mut props = json!({"DeliveryStreamName": "events", "Tags": [{"Key": "env", "Value": "before"}]});
            props[shape] = configuration();
            let resource = make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                props.clone(),
            );
            let existing = prov.create_resource(&resource).unwrap();
            let before = stored(&prov);
            let original_dest = before.destination.as_ref().unwrap();
            assert_eq!(original_dest.file_extension.as_deref(), Some(".data"));
            assert_eq!(original_dest.compression_format.as_deref(), Some("ZIP"));
            assert_eq!(
                original_dest.processing_configuration.as_ref().unwrap()["Enabled"],
                true
            );
            assert_eq!(before.version_id, "1");
            props[shape]["Prefix"] = json!("after/");
            props[shape]["CompressionFormat"] = json!("GZIP");
            props[shape]["FileExtension"] = json!(".updated");
            props[shape]["BufferingHints"] = json!({"SizeInMBs": 128, "IntervalInSeconds": 1});
            props["Tags"][0]["Value"] = json!("after");
            let updated = prov
                .update_resource(
                    &existing,
                    &make_resource("AWS::KinesisFirehose::DeliveryStream", "Firehose", props),
                )
                .unwrap()
                .unwrap();
            let after = stored(&prov);
            let dest = after.destination.as_ref().unwrap();
            assert_eq!(after.instance_id, before.instance_id);
            assert_eq!(after.created_at, before.created_at);
            assert_eq!(after.arn, before.arn);
            assert_eq!(updated.physical_id, existing.physical_id);
            assert_eq!(dest.destination_id, original_dest.destination_id);
            assert_eq!(dest.prefix.as_deref(), Some("after/"));
            assert_eq!(dest.file_extension.as_deref(), Some(".updated"));
            assert_eq!(dest.compression_format.as_deref(), Some("GZIP"));
            assert_eq!(
                dest.processing_configuration,
                original_dest.processing_configuration
            );
            assert_eq!(dest.buffering_size_mb, Some(128));
            assert_eq!(dest.buffering_interval_seconds, Some(1));
            assert_eq!(after.version_id, "2");
            assert_eq!(after.tags.get("env").map(String::as_str), Some("after"));
        }
    }

    #[test]
    fn firehose_invalid_create_update_is_atomic() {
        let prov = make_provisioner();
        let props = json!({"DeliveryStreamName": "events", "ExtendedS3DestinationConfiguration": configuration(), "Tags": [{"Key": "env", "Value": "before"}]});
        let existing = prov
            .create_resource(&make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                props.clone(),
            ))
            .unwrap();
        let before = stored(&prov);
        for bad in [
            json!({"FileExtension": "bad"}),
            json!({"FileExtension": ".JSON"}),
            json!({"FileExtension": 1}),
            json!({"BufferingHints": {"SizeInMBs": 1}}),
            json!({"BufferingHints": {"SizeInMBs": 0, "IntervalInSeconds": 300}}),
            json!({"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": "300s"}}),
        ] {
            let expected = if bad.get("FileExtension").is_some() {
                "FileExtension"
            } else {
                "BufferingHints"
            };
            let mut invalid = props.clone();
            invalid["ExtendedS3DestinationConfiguration"]
                .as_object_mut()
                .unwrap()
                .extend(bad.as_object().unwrap().clone());
            invalid["ExtendedS3DestinationConfiguration"]["Prefix"] = json!("changed/");
            invalid["Tags"][0]["Value"] = json!("changed");
            let resource = make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                invalid.clone(),
            );
            let error = prov.update_resource(&existing, &resource).unwrap_err();
            assert!(error.contains(expected), "{error}");
            let after = stored(&prov);
            assert_eq!(after.instance_id, before.instance_id);
            assert_eq!(
                serde_json::to_value(after).unwrap(),
                serde_json::to_value(&before).unwrap()
            );
            invalid["DeliveryStreamName"] = json!("invalid-create");
            let error = prov
                .create_resource(&make_resource(
                    "AWS::KinesisFirehose::DeliveryStream",
                    "BadFirehose",
                    invalid,
                ))
                .unwrap_err();
            assert!(error.contains(expected), "{error}");
            assert!(!prov
                .firehose_state
                .read()
                .get("123456789012")
                .unwrap()
                .streams("us-east-1")
                .unwrap()
                .contains_key("invalid-create"));
        }
    }
    #[test]
    fn firehose_cfn_stores_processing_configuration_on_create_and_update() {
        let prov = make_provisioner();
        let delivery_stream = |destination: Value| {
            make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                json!({"DeliveryStreamName": "events", "ExtendedS3DestinationConfiguration": destination}),
            )
        };
        let with_processing = |enabled: Value| {
            json!({
                "RoleARN": "arn:aws:iam::123456789012:role/fh",
                "BucketARN": "arn:aws:s3:::bucket",
                "ProcessingConfiguration": {
                    "Enabled": enabled,
                    "Processors": [{"Type": "AppendDelimiterToRecord"}]
                }
            })
        };
        let stored = |prov: &ResourceProvisioner| {
            prov.firehose_state
                .read()
                .get("123456789012")
                .unwrap()
                .streams("us-east-1")
                .unwrap()["events"]
                .destination
                .as_ref()
                .unwrap()
                .processing_configuration
                .clone()
                .expect("ProcessingConfiguration stored")
        };

        let existing = prov
            .create_resource(&delivery_stream(with_processing(json!(true))))
            .unwrap();
        assert_eq!(stored(&prov)["Enabled"], json!(true));
        assert_eq!(
            stored(&prov)["Processors"][0]["Type"],
            "AppendDelimiterToRecord"
        );

        // Template values can arrive as strings; they are stored as booleans.
        prov.update_resource(&existing, &delivery_stream(with_processing(json!("false"))))
            .unwrap();
        assert_eq!(stored(&prov)["Enabled"], json!(false));
    }

    #[test]
    fn firehose_cfn_modeled_settings_survive_create_and_update() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            let prov = make_provisioner();
            let mut existing = None;
            for (enabled, normalized, zone, backup_mode, prefix) in [
                ("TrUe", true, "America/New_York", "Enabled", "before/"),
                ("FaLsE", false, "UTC", "Disabled", "after/"),
            ] {
                let mut resource = firehose_resource(base);
                resource.properties[base] = json!({
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "Prefix": prefix,
                    "CompressionFormat": "GZIP",
                    "ProcessingConfiguration": {
                        "Enabled": enabled,
                        "Processors": [{"Type": "AppendDelimiterToRecord"}]
                    },
                    "DataFormatConversionConfiguration": {
                        "Enabled": enabled,
                        "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {}}}
                    },
                    "DynamicPartitioningConfiguration": {
                        "Enabled": enabled, "RetryOptions": {"DurationInSeconds": 60}
                    },
                    "CloudWatchLoggingOptions": {
                        "Enabled": enabled, "LogGroupName": "delivery", "LogStreamName": prefix
                    },
                    "CustomTimeZone": zone,
                    "S3BackupMode": backup_mode,
                    "EncryptionConfiguration": {"KMSEncryptionConfig": {"AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:key/test"}},
                    "S3BackupConfiguration": {
                        "RoleARN": "arn:aws:iam::123456789012:role/backup",
                        "BucketARN": "arn:aws:s3:::backup",
                        "Prefix": prefix,
                        "CompressionFormat": "UNCOMPRESSED",
                        "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
                        "CloudWatchLoggingOptions": {"Enabled": false}
                    }
                });
                if let Some(existing) = &existing {
                    prov.update_resource(existing, &resource)
                        .unwrap()
                        .expect("in-place update");
                } else {
                    existing = Some(prov.create_resource(&resource).unwrap());
                }
                let state = prov.firehose_state.read();
                let stream = &state
                    .get("123456789012")
                    .unwrap()
                    .streams("us-east-1")
                    .unwrap()["cfn-compression"];
                let dest = stream.destination.as_ref().unwrap();
                assert_eq!(
                    dest.data_format_conversion_configuration,
                    Some(json!({
                        "Enabled": normalized,
                        "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {}}}
                    }))
                );
                assert_eq!(
                    dest.processing_configuration,
                    Some(json!({
                        "Enabled": normalized,
                        "Processors": [{"Type": "AppendDelimiterToRecord"}]
                    }))
                );
                assert_eq!(
                    dest.dynamic_partitioning_configuration,
                    Some(json!({
                        "Enabled": normalized, "RetryOptions": {"DurationInSeconds": 60}
                    }))
                );
                assert_eq!(
                    dest.cloudwatch_logging_options,
                    Some(json!({
                        "Enabled": normalized, "LogGroupName": "delivery", "LogStreamName": prefix
                    }))
                );
                assert_eq!(dest.custom_time_zone.as_deref(), Some(zone));
                assert_eq!(dest.s3_backup_mode.as_deref(), Some(backup_mode));
                assert_eq!(
                    dest.encryption_configuration,
                    Some(json!({
                        "KMSEncryptionConfig": {"AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:key/test"}
                    }))
                );
                assert_eq!(
                    dest.s3_backup_description,
                    Some(json!({
                        "RoleARN": "arn:aws:iam::123456789012:role/backup",
                        "BucketARN": "arn:aws:s3:::backup",
                        "Prefix": prefix,
                        "CompressionFormat": "UNCOMPRESSED",
                        "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
                        "CloudWatchLoggingOptions": {"Enabled": false}
                    }))
                );
                assert_eq!(dest.prefix.as_deref(), Some(prefix));
                assert_eq!(dest.compression_format.as_deref(), Some("GZIP"));
                assert_eq!(stream.version_id, if normalized { "1" } else { "2" });
            }
        }
    }

    #[test]
    fn firehose_cfn_modeled_settings_defaults_and_disabled_blocks() {
        let base = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket", "FileExtension": ".json",
        });
        let dest =
            parse_firehose_s3_destination(&base, "ExtendedS3DestinationConfiguration").unwrap();
        assert!(dest.processing_configuration.is_none());
        assert!(dest.data_format_conversion_configuration.is_none());
        assert!(dest.dynamic_partitioning_configuration.is_none());
        assert!(dest.cloudwatch_logging_options.is_none());
        assert!(dest.custom_time_zone.is_none());
        assert!(dest.s3_backup_mode.is_none());
        assert!(dest.encryption_configuration.is_none());
        assert!(dest.s3_backup_description.is_none());

        for enabled in [None, Some(json!(false)), Some(json!("false"))] {
            let mut config = base.clone();
            for field in [
                "ProcessingConfiguration",
                "DataFormatConversionConfiguration",
                "DynamicPartitioningConfiguration",
                "CloudWatchLoggingOptions",
            ] {
                config[field] = match &enabled {
                    Some(enabled) => json!({"Enabled": enabled}),
                    None => json!({}),
                };
            }
            let dest = parse_firehose_s3_destination(&config, "ExtendedS3DestinationConfiguration")
                .unwrap();
            let expected = if enabled.is_some() {
                json!({"Enabled": false})
            } else {
                json!({})
            };
            assert_eq!(dest.processing_configuration, Some(expected.clone()));
            assert_eq!(
                dest.data_format_conversion_configuration,
                Some(expected.clone())
            );
            assert_eq!(
                dest.dynamic_partitioning_configuration,
                Some(expected.clone())
            );
            assert_eq!(dest.cloudwatch_logging_options, Some(expected));
        }
    }

    /// `Ref` parameters resolve to strings, and AWS's own templates quote
    /// numbers and booleans. Every nested modeled boolean and number,
    /// including the backup configuration's, is stored with its API type (so
    /// DescribeDeliveryStream echoes what SDKs deserialize), while string
    /// members and free-form map values are left untouched.
    #[test]
    fn firehose_cfn_coerces_template_scalars() {
        let prov = make_provisioner();
        prov.create_resource(&delivery_stream(json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket",
            "Prefix": "60",
            "BufferingHints": {"SizeInMBs": "64", "IntervalInSeconds": "60"},
            "CloudWatchLoggingOptions": {"Enabled": "false", "LogGroupName": "true"},
            "ProcessingConfiguration": {
                "Enabled": "True",
                "Processors": [{
                    "Type": "AppendDelimiterToRecord",
                    "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "60"}]
                }]
            },
            "DataFormatConversionConfiguration": {
                "Enabled": "false",
                "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {
                    "CaseInsensitive": "true",
                    "ConvertDotsInJsonKeysToUnderscores": "FALSE",
                    "ColumnToJsonKeyMappings": {"Enabled": "true", "SizeInMBs": "1"}
                }}},
                "OutputFormatConfiguration": {"Serializer": {"OrcSerDe": {
                    "StripeSizeBytes": "8388608",
                    "BlockSizeBytes": "67108864",
                    "RowIndexStride": "1000",
                    "EnablePadding": "true",
                    "PaddingTolerance": "0.05",
                    "BloomFilterFalsePositiveProbability": "0.1",
                    "DictionaryKeyThreshold": "0.5",
                    "BloomFilterColumns": ["60"]
                }}}
            },
            "DynamicPartitioningConfiguration": {
                "Enabled": "false", "RetryOptions": {"DurationInSeconds": "300"}
            },
            "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
            "S3BackupConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/backup",
                "BucketARN": "arn:aws:s3:::backup",
                "BufferingHints": {"SizeInMBs": "1", "IntervalInSeconds": "0"},
                "CloudWatchLoggingOptions": {"Enabled": "TRUE", "LogGroupName": "backup"}
            }
        })))
        .unwrap();

        let state = prov.firehose_state.read();
        let dest = state
            .get("123456789012")
            .unwrap()
            .streams("us-east-1")
            .unwrap()["events"]
            .destination
            .clone()
            .unwrap();
        assert_eq!(dest.prefix.as_deref(), Some("60"));
        assert_eq!(dest.buffering_size_mb, Some(64));
        assert_eq!(dest.buffering_interval_seconds, Some(60));
        assert_eq!(
            dest.cloudwatch_logging_options,
            Some(json!({"Enabled": false, "LogGroupName": "true"}))
        );
        let processing = dest.processing_configuration.unwrap();
        assert_eq!(processing["Enabled"], json!(true));
        assert_eq!(
            processing["Processors"][0]["Parameters"][0]["ParameterValue"],
            json!("60")
        );
        let conversion = dest.data_format_conversion_configuration.unwrap();
        assert_eq!(conversion["Enabled"], json!(false));
        let openx = &conversion["InputFormatConfiguration"]["Deserializer"]["OpenXJsonSerDe"];
        assert_eq!(openx["CaseInsensitive"], json!(true));
        assert_eq!(openx["ConvertDotsInJsonKeysToUnderscores"], json!(false));
        assert_eq!(
            openx["ColumnToJsonKeyMappings"],
            json!({"Enabled": "true", "SizeInMBs": "1"})
        );
        assert_eq!(
            conversion["OutputFormatConfiguration"]["Serializer"]["OrcSerDe"],
            json!({
                "StripeSizeBytes": 8388608,
                "BlockSizeBytes": 67108864,
                "RowIndexStride": 1000,
                "EnablePadding": true,
                "PaddingTolerance": 0.05,
                "BloomFilterFalsePositiveProbability": 0.1,
                "DictionaryKeyThreshold": 0.5,
                "BloomFilterColumns": ["60"]
            })
        );
        assert_eq!(
            dest.dynamic_partitioning_configuration,
            Some(json!({"Enabled": false, "RetryOptions": {"DurationInSeconds": 300}}))
        );
        assert_eq!(
            dest.encryption_configuration,
            Some(json!({"NoEncryptionConfig": "NoEncryption"}))
        );
        assert_eq!(
            dest.s3_backup_description,
            Some(json!({
                "RoleARN": "arn:aws:iam::123456789012:role/backup",
                "BucketARN": "arn:aws:s3:::backup",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
                "CloudWatchLoggingOptions": {"Enabled": true, "LogGroupName": "backup"}
            }))
        );
    }

    /// A modeled boolean or number whose template string does not parse
    /// exactly, or whose JSON value has the wrong type, fails the resource
    /// (as CloudFormation's schema validation does) instead of being stored
    /// as a value DescribeDeliveryStream would echo where SDKs expect another
    /// type, and leaves no stream behind. The error names the full member
    /// path, starting at the top-level property.
    #[test]
    fn firehose_cfn_unparseable_template_scalar_fails_resource() {
        const ROOT: &str = "ExtendedS3DestinationConfiguration";
        const INTERVAL: &str = "/S3BackupConfiguration/BufferingHints/IntervalInSeconds";
        const SIZE: &str = "/BufferingHints/SizeInMBs";
        const LOGGING: &str = "/CloudWatchLoggingOptions/Enabled";
        const PROCESSING: &str = "/ProcessingConfiguration/Enabled";
        const PADDING: &str = "/DataFormatConversionConfiguration/OutputFormatConfiguration/Serializer/OrcSerDe/PaddingTolerance";
        let integer = "must be a 32-bit integer";
        let boolean = "must be a boolean";
        let number = "must be a number";
        for (pointer, bad, expected) in [
            (INTERVAL, json!("not-a-number"), integer),
            (SIZE, json!("5.5"), integer),
            (SIZE, json!(" 1 "), integer),
            (SIZE, json!("2147483648"), integer),
            (SIZE, json!(2_147_483_648_i64), integer),
            (SIZE, json!(5.5), integer),
            (SIZE, json!(true), integer),
            (SIZE, json!({"Value": 5}), integer),
            (LOGGING, json!("yes"), boolean),
            (LOGGING, json!("true "), boolean),
            (LOGGING, json!(1), boolean),
            (LOGGING, json!({"Value": true}), boolean),
            (PROCESSING, json!("1"), boolean),
            (PADDING, json!("lots"), number),
            (PADDING, json!(" 0.05"), number),
            (PADDING, json!(true), number),
            (PADDING, json!({"Value": 0.05}), number),
        ] {
            let message = format!("{ROOT}{} {expected}, got {bad}", pointer.replace('/', "."));
            let case = format!("{pointer} = {bad}");
            let mut config = json!({
                "RoleARN": "arn:aws:iam::123456789012:role/fh",
                "BucketARN": "arn:aws:s3:::bucket",
                "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
                "CloudWatchLoggingOptions": {"Enabled": true},
                "ProcessingConfiguration": {"Enabled": false},
                "DataFormatConversionConfiguration": {"OutputFormatConfiguration": {
                    "Serializer": {"OrcSerDe": {"PaddingTolerance": 0.05}}
                }},
                "S3BackupConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/backup",
                    "BucketARN": "arn:aws:s3:::backup",
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300}
                }
            });
            *config.pointer_mut(pointer).unwrap() = bad.clone();
            let prov = make_provisioner();
            let err = prov
                .create_resource(&delivery_stream(config.clone()))
                .expect_err("unparseable scalar must fail provisioning");
            assert!(err.contains(&message), "{case}: {err}");
            assert!(prov.firehose_state.read().accounts.is_empty(), "{case}");

            // The same template on update leaves the original stream alone.
            let prov = make_provisioner();
            *config.pointer_mut(pointer).unwrap() = match pointer.rsplit('/').next() {
                Some("Enabled") => json!(false),
                Some("PaddingTolerance") => json!(0.05),
                _ => json!(60),
            };
            let existing = prov
                .create_resource(&delivery_stream(config.clone()))
                .unwrap();
            let before = firehose_state(&prov);
            *config.pointer_mut(pointer).unwrap() = bad;
            let err = prov
                .update_resource(&existing, &delivery_stream(config))
                .expect_err("unparseable scalar must fail the update");
            assert!(err.contains(&message), "{case}: {err}");
            assert_eq!(firehose_state(&prov), before, "{case}");
        }
    }

    /// Values that already have their modeled JSON type pass through: any
    /// integral number (within 32 bits) for an integer member, any number
    /// for a double member. The error path starts at the property named by
    /// the caller.
    #[test]
    fn firehose_cfn_accepts_typed_json_scalars() {
        let coerced = coerce_firehose_scalars(
            json!({
                "BufferingHints": {"SizeInMBs": 2_147_483_647, "IntervalInSeconds": 60.0},
                "CloudWatchLoggingOptions": {"Enabled": false},
                "OrcSerDe": {"PaddingTolerance": 1, "DictionaryKeyThreshold": 0.5}
            }),
            "S3DestinationConfiguration",
        )
        .unwrap();
        assert_eq!(
            coerced,
            json!({
                "BufferingHints": {"SizeInMBs": 2_147_483_647, "IntervalInSeconds": 60},
                "CloudWatchLoggingOptions": {"Enabled": false},
                "OrcSerDe": {"PaddingTolerance": 1, "DictionaryKeyThreshold": 0.5}
            })
        );
        assert_eq!(
            coerce_firehose_scalars(
                json!({"BufferingHints": {"SizeInMBs": -2_147_483_649_i64}}),
                "S3DestinationConfiguration",
            )
            .unwrap_err(),
            "S3DestinationConfiguration.BufferingHints.SizeInMBs must be a 32-bit integer, \
             got -2147483649"
        );
    }

    fn firehose_resource(base: &str) -> ResourceDefinition {
        let mut props = json!({
            "DeliveryStreamName": "cfn-compression",
            "Tags": [{"Key": "env", "Value": "before"}]
        });
        props[base] = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket", "Prefix": "before/"
        });
        make_resource("AWS::KinesisFirehose::DeliveryStream", "Stream", props)
    }

    fn firehose_state(prov: &ResourceProvisioner) -> Value {
        serde_json::to_value(&*prov.firehose_state.read()).unwrap()
    }

    #[test]
    fn firehose_compression_invalid_create_is_resource_failure() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            for bad in [
                json!("BROTLI"),
                json!("SNAPPY"),
                json!(null),
                json!(123),
                json!({}),
            ] {
                let prov = make_provisioner();
                let before = firehose_state(&prov);
                let mut resource = firehose_resource(base);
                resource.properties[base]["CompressionFormat"] = bad;
                let err = prov
                    .create_resource(&resource)
                    .expect_err("invalid enum must fail provisioning");
                assert!(err.contains("CompressionFormat"), "{err}");
                assert_eq!(firehose_state(&prov), before);
            }
        }
    }

    #[test]
    fn firehose_compression_invalid_update_preserves_original_stream() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            let prov = make_provisioner();
            let resource = firehose_resource(base);
            let existing = prov.create_resource(&resource).unwrap();
            let before = firehose_state(&prov);
            for bad in [json!("BROTLI"), json!("gzip"), json!(false)] {
                let mut changed = firehose_resource(base);
                changed.properties[base]["Prefix"] = json!("changed/");
                changed.properties[base]["CompressionFormat"] = bad;
                changed.properties["Tags"] = json!([{ "Key": "env", "Value": "changed" }]);
                assert!(prov.update_resource(&existing, &changed).is_err());
                assert_eq!(firehose_state(&prov), before);
            }
        }
    }

    #[test]
    fn firehose_compression_invalid_backup_update_preserves_original_stream() {
        let prov = make_provisioner();
        let base = "ExtendedS3DestinationConfiguration";
        let resource = firehose_resource(base);
        let existing = prov.create_resource(&resource).unwrap();
        let before = firehose_state(&prov);
        let mut changed = firehose_resource(base);
        changed.properties[base]["Prefix"] = json!("changed/");
        changed.properties[base]["S3BackupConfiguration"] = json!({"CompressionFormat": 42});
        assert!(prov.update_resource(&existing, &changed).is_err());
        assert_eq!(firehose_state(&prov), before);
    }

    #[test]
    fn firehose_compression_backup_validated_for_both_s3_shapes() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            let prov = make_provisioner();
            let mut resource = firehose_resource(base);
            resource.properties[base]["S3BackupConfiguration"] =
                json!({"CompressionFormat": "BROTLI"});
            let err = prov
                .create_resource(&resource)
                .expect_err("invalid backup enum must fail provisioning");
            assert!(err.contains("CompressionFormat"), "{base}: {err}");
            assert!(prov.firehose_state.read().accounts.is_empty());
        }
    }

    #[test]
    fn firehose_compression_invalid_backup_create_is_resource_failure() {
        let prov = make_provisioner();
        let mut resource = firehose_resource("ExtendedS3DestinationConfiguration");
        resource.properties["ExtendedS3DestinationConfiguration"]["S3BackupConfiguration"] =
            json!({"CompressionFormat": "BROTLI"});
        assert!(prov.create_resource(&resource).is_err());
        assert!(prov.firehose_state.read().accounts.is_empty());
    }

    #[test]
    fn firehose_compression_valid_values_and_defaults_provision() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            for format in [
                None,
                Some("UNCOMPRESSED"),
                Some("GZIP"),
                Some("ZIP"),
                Some("Snappy"),
                Some("HADOOP_SNAPPY"),
            ] {
                let prov = make_provisioner();
                let mut resource = firehose_resource(base);
                if let Some(format) = format {
                    resource.properties[base]["CompressionFormat"] = json!(format);
                }
                let existing = prov.create_resource(&resource).unwrap();
                prov.update_resource(&existing, &resource)
                    .unwrap()
                    .expect("in-place update");
                let state = prov.firehose_state.read();
                let stream = &state
                    .get("123456789012")
                    .unwrap()
                    .streams("us-east-1")
                    .unwrap()["cfn-compression"];
                assert_eq!(
                    stream
                        .destination
                        .as_ref()
                        .unwrap()
                        .compression_format
                        .as_deref(),
                    format
                );
                assert_eq!(stream.version_id, "2");
            }
        }
    }

    #[tokio::test]
    async fn firehose_delete_recreate_invalidates_queued_and_captured_delivery() {
        use fakecloud_firehose::writer::{BatchWriter, DeliveryBatch, S3BatchWriter};
        use fakecloud_firehose::{delivery_config, FirehoseEngine, StreamKey};
        use fakecloud_s3::delivery::DeliveryWriteOutcome;

        let prov = make_provisioner();
        prov.s3_state
            .write()
            .get_or_create("123456789012")
            .buckets
            .insert(
                "bucket".into(),
                fakecloud_s3::S3Bucket::new("bucket", "us-east-1", "owner"),
            );
        let resource = make_resource(
            "AWS::KinesisFirehose::DeliveryStream",
            "Firehose",
            json!({
                "DeliveryStreamName": "events",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket",
                    "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0}
                }
            }),
        );
        prov.create_resource(&resource).unwrap();
        let before = stored(&prov);
        let engine = Arc::new(FirehoseEngine::new(prov.firehose_state.clone()));
        engine.set_s3(prov.s3_state.clone());
        engine
            .enqueue(
                "123456789012",
                "us-east-1",
                "events",
                vec![bytes::Bytes::from_static(b"old")],
            )
            .unwrap();
        let batch = DeliveryBatch {
            stream: StreamKey {
                account_id: "123456789012".into(),
                region: "us-east-1".into(),
                name: "events".into(),
                instance_id: before.instance_id,
            },
            version_id: "1".into(),
            destination: delivery_config(before.destination.as_ref().unwrap()).unwrap(),
            records: vec![bytes::Bytes::from_static(b"captured-old")],
            document_ids: Vec::new(),
            oldest_arrival: Utc::now(),
            delivery_time: Utc::now(),
            object_key: "stale".into(),
        };
        prov.delete_firehose_delivery_stream("events").unwrap();
        let writer = S3BatchWriter::new(
            prov.firehose_state.clone(),
            prov.s3_state.clone(),
            Arc::new(parking_lot::RwLock::new(None)),
        );
        assert_eq!(
            writer.write(&batch).unwrap(),
            DeliveryWriteOutcome::Cancelled
        );
        prov.create_resource(&resource).unwrap();
        assert_ne!(stored(&prov).instance_id, before.instance_id);
        assert_eq!(
            writer.write(&batch).unwrap(),
            DeliveryWriteOutcome::Cancelled
        );
        engine
            .enqueue(
                "123456789012",
                "us-east-1",
                "events",
                vec![bytes::Bytes::from_static(b"new")],
            )
            .unwrap();
        assert_eq!(
            engine.shutdown().await,
            fakecloud_firehose::DrainReport {
                delivered_batches: 1,
                undelivered_records: 0,
                undelivered_bytes: 0,
            }
        );
        let state = prov.s3_state.read();
        let objects = &state.get("123456789012").unwrap().buckets["bucket"].objects;
        assert_eq!(objects.len(), 1);
        let fakecloud_persistence::BodyRef::Memory(body) = &objects.values().next().unwrap().body
        else {
            panic!("memory fixture")
        };
        assert_eq!(body.as_ref(), b"new");
    }
}
