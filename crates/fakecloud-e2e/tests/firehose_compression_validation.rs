use aws_sdk_firehose::error::ProvideErrorMetadata;
use aws_sdk_firehose::types::{
    CompressionFormat, ExtendedS3DestinationConfiguration, ExtendedS3DestinationUpdate,
    S3DestinationUpdate,
};
use fakecloud_testkit::TestServer;

async fn assert_unsupported_admission(
    client: &aws_sdk_firehose::Client,
    name: &str,
    setting: &str,
) {
    use aws_sdk_firehose::{primitives::Blob, types::Record};
    let error = client
        .put_record()
        .delivery_stream_name(name)
        .record(
            Record::builder()
                .data(Blob::new(b"reject"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
    let error = error.as_service_error().unwrap();
    assert_eq!(error.code(), Some("InvalidArgumentException"));
    assert!(error.message().unwrap().contains(setting), "{error}");
    let error = client
        .put_record_batch()
        .delivery_stream_name(name)
        .records(
            Record::builder()
                .data(Blob::new(b"reject-batch"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
    let error = error.as_service_error().unwrap();
    assert_eq!(error.code(), Some("InvalidArgumentException"));
    assert!(error.message().unwrap().contains(setting), "{error}");
}

#[tokio::test]
async fn firehose_mixed_destinations_do_not_bypass_admission() {
    use aws_sdk_firehose::types::{
        HttpEndpointConfiguration, HttpEndpointDestinationConfiguration,
    };
    let server = TestServer::start().await;
    let firehose = server.firehose_client().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("compression-bucket")
        .send()
        .await
        .unwrap();
    s3.put_bucket_versioning()
        .bucket("compression-bucket")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    for add_s3_on_update in [false, true] {
        let name = format!("mixed-{add_s3_on_update}");
        let mut create = firehose
            .create_delivery_stream()
            .delivery_stream_name(&name)
            .http_endpoint_destination_configuration(
                HttpEndpointDestinationConfiguration::builder()
                    .endpoint_configuration(
                        HttpEndpointConfiguration::builder()
                            .url("https://example.invalid/ingest")
                            .build()
                            .unwrap(),
                    )
                    .role_arn("arn:aws:iam::123456789012:role/fh")
                    .build(),
            );
        if !add_s3_on_update {
            create = create.extended_s3_destination_configuration(destination("UNCOMPRESSED"));
        }
        create.send().await.unwrap();
        if add_s3_on_update {
            let description = firehose
                .describe_delivery_stream()
                .delivery_stream_name(&name)
                .send()
                .await
                .unwrap();
            let description = description.delivery_stream_description().unwrap();
            firehose
                .update_destination()
                .delivery_stream_name(&name)
                .current_delivery_stream_version_id(description.version_id())
                .destination_id(description.destinations()[0].destination_id())
                .extended_s3_destination_update(
                    ExtendedS3DestinationUpdate::builder()
                        .role_arn("arn:aws:iam::123456789012:role/fh")
                        .bucket_arn("arn:aws:s3:::compression-bucket")
                        .build(),
                )
                .send()
                .await
                .unwrap();
        }
        assert_unsupported_admission(&firehose, &name, "HttpEndpointDestinationConfiguration")
            .await;
    }
    let versions = s3
        .list_object_versions()
        .bucket("compression-bucket")
        .send()
        .await
        .unwrap();
    assert!(versions.versions().is_empty());
    assert!(versions.delete_markers().is_empty());
}

#[tokio::test]
async fn firehose_cfn_preserves_unsupported_settings_for_admission() {
    use serde_json::json;
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let firehose = server.firehose_client().await;
    for (i, setting) in [
        json!({"CompressionFormat": "ZIP"}),
        json!({"ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "Lambda"}]}}),
        json!({"DataFormatConversionConfiguration": {"Enabled": true}}),
        json!({"DataFormatConversionConfiguration": {"SchemaConfiguration": {}}}),
        json!({"DynamicPartitioningConfiguration": {"Enabled": true}}),
        json!({"CustomTimeZone": "America/New_York"}),
        json!({"S3BackupMode": "Enabled"}),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("unsupported-cfn-{i}");
        let field = setting.as_object().unwrap().keys().next().unwrap();
        let mut dest = json!({"RoleARN": "arn:aws:iam::123456789012:role/fh", "BucketARN": "arn:aws:s3:::missing"});
        dest.as_object_mut()
            .unwrap()
            .extend(setting.as_object().unwrap().clone());
        let template = json!({"Resources": {"Stream": {"Type": "AWS::KinesisFirehose::DeliveryStream", "Properties": {
            "DeliveryStreamName": name, "ExtendedS3DestinationConfiguration": dest,
        }}}});
        cfn.create_stack()
            .stack_name(&name)
            .template_body(template.to_string())
            .send()
            .await
            .unwrap();
        assert_unsupported_admission(&firehose, &name, field).await;
        // A CFN update retains the unsupported setting while changing another field.
        let mut updated = template;
        updated["Resources"]["Stream"]["Properties"]["ExtendedS3DestinationConfiguration"]
            ["Prefix"] = json!("after/");
        cfn.update_stack()
            .stack_name(&name)
            .template_body(updated.to_string())
            .send()
            .await
            .unwrap();
        assert_unsupported_admission(&firehose, &name, field).await;
    }
}

fn destination(format: &str) -> ExtendedS3DestinationConfiguration {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose")
        .bucket_arn("arn:aws:s3:::compression-bucket")
        .prefix("before/")
        .compression_format(CompressionFormat::from(format))
        .build()
        .unwrap()
}

async fn firehose_cfn_invalid_retry(change_set: bool) {
    use aws_sdk_cloudformation::types::{ChangeSetType, Parameter, Tag};
    use serde_json::json;

    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let firehose = server.firehose_client().await;
    let template = json!({
        "Parameters": {
            "Codec": {"Type": "String", "Default": "UNCOMPRESSED"},
            "Prefix": {"Type": "String", "Default": "default/"}
        },
        "Resources": {"Stream": {
            "Type": "AWS::KinesisFirehose::DeliveryStream",
            "Properties": {
                "DeliveryStreamName": "sdk-cfn-compression",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "Prefix": {"Ref": "Prefix"},
                    "CompressionFormat": {"Ref": "Codec"}
                }
            }
        }},
        "Outputs": {"Codec": {"Value": {"Ref": "Codec"}}}
    });
    let original = template.to_string();
    cfn.create_stack()
        .stack_name("compression-stack")
        .template_body(&original)
        .parameters(
            Parameter::builder()
                .parameter_key("Codec")
                .parameter_value("GZIP")
                .build(),
        )
        .parameters(
            Parameter::builder()
                .parameter_key("Prefix")
                .parameter_value("before/")
                .build(),
        )
        .tags(Tag::builder().key("env").value("before").build())
        .send()
        .await
        .unwrap();
    let before = cfn
        .describe_stacks()
        .stack_name("compression-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        before.stacks()[0].stack_status().unwrap().as_str(),
        "CREATE_COMPLETE"
    );
    let firehose_before = firehose
        .describe_delivery_stream()
        .delivery_stream_name("sdk-cfn-compression")
        .send()
        .await
        .unwrap();
    let mut invalid = template.clone();
    invalid["Resources"]["Stream"]["Properties"]["ExtendedS3DestinationConfiguration"]
        ["CompressionFormat"] = json!("BROTLI");
    let invalid = invalid.to_string();
    for (attempt, name) in ["invalid-1", "invalid-2"].into_iter().enumerate() {
        let parameters = vec![
            Parameter::builder()
                .parameter_key("Codec")
                .parameter_value("ZIP")
                .build(),
            Parameter::builder()
                .parameter_key("Prefix")
                .use_previous_value(true)
                .build(),
        ];
        if change_set {
            cfn.create_change_set()
                .stack_name("compression-stack")
                .change_set_name(name)
                .change_set_type(ChangeSetType::Update)
                .template_body(&invalid)
                .set_parameters(Some(parameters))
                .tags(Tag::builder().key("env").value("rejected").build())
                .send()
                .await
                .unwrap();
            // Accepted, as on AWS: the stack rolls back and its events say why.
            cfn.execute_change_set()
                .stack_name("compression-stack")
                .change_set_name(name)
                .send()
                .await
                .unwrap();
        } else {
            cfn.update_stack()
                .stack_name("compression-stack")
                .template_body(&invalid)
                .set_parameters(Some(parameters))
                .tags(Tag::builder().key("env").value("rejected").build())
                .send()
                .await
                .unwrap();
        }
        let after = cfn
            .describe_stacks()
            .stack_name("compression-stack")
            .send()
            .await
            .unwrap();
        let stack = &after.stacks()[0];
        assert_eq!(
            stack.stack_status().unwrap().as_str(),
            "UPDATE_ROLLBACK_COMPLETE"
        );
        // Each retry reached validation again, rather than being skipped as
        // unchanged: one more failed event each time.
        let events = cfn
            .describe_stack_events()
            .stack_name("compression-stack")
            .send()
            .await
            .unwrap();
        let failures = events
            .stack_events()
            .iter()
            .filter(|e| {
                e.resource_status()
                    .is_some_and(|s| s.as_str() == "UPDATE_FAILED")
                    && e.resource_status_reason()
                        .is_some_and(|r| r.contains("CompressionFormat"))
            })
            .count();
        assert_eq!(failures, attempt + 1, "{name}");
        assert_eq!(stack.parameters(), before.stacks()[0].parameters());
        assert_eq!(stack.tags(), before.stacks()[0].tags());
        assert_eq!(stack.outputs(), before.stacks()[0].outputs());
        let stored = cfn
            .get_template()
            .stack_name("compression-stack")
            .send()
            .await
            .unwrap();
        assert_eq!(stored.template_body(), Some(original.as_str()));
        let firehose_after = firehose
            .describe_delivery_stream()
            .delivery_stream_name("sdk-cfn-compression")
            .send()
            .await
            .unwrap();
        assert_eq!(
            firehose_after.delivery_stream_description(),
            firehose_before.delivery_stream_description()
        );
    }

    let parameters = vec![
        Parameter::builder()
            .parameter_key("Codec")
            .parameter_value("Snappy")
            .build(),
        Parameter::builder()
            .parameter_key("Prefix")
            .use_previous_value(true)
            .build(),
    ];
    if change_set {
        cfn.create_change_set()
            .stack_name("compression-stack")
            .change_set_name("valid")
            .change_set_type(ChangeSetType::Update)
            .template_body(&original)
            .set_parameters(Some(parameters))
            .send()
            .await
            .unwrap();
        cfn.execute_change_set()
            .stack_name("compression-stack")
            .change_set_name("valid")
            .send()
            .await
            .unwrap();
    } else {
        cfn.update_stack()
            .stack_name("compression-stack")
            .template_body(&original)
            .set_parameters(Some(parameters))
            .send()
            .await
            .unwrap();
    }
    let after = cfn
        .describe_stacks()
        .stack_name("compression-stack")
        .send()
        .await
        .unwrap();
    let stack = &after.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "UPDATE_COMPLETE");
    assert!(stack.stack_status_reason().is_none());
    assert_eq!(stack.outputs()[0].output_value(), Some("Snappy"));
    let stored = cfn
        .get_template()
        .stack_name("compression-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(stored.template_body(), Some(original.as_str()));
    assert_eq!(
        stack
            .parameters()
            .iter()
            .find(|p| p.parameter_key() == Some("Codec"))
            .unwrap()
            .parameter_value(),
        Some("Snappy")
    );
    assert_eq!(
        stack
            .parameters()
            .iter()
            .find(|p| p.parameter_key() == Some("Prefix"))
            .unwrap()
            .parameter_value(),
        Some("before/")
    );
    let desc = firehose
        .describe_delivery_stream()
        .delivery_stream_name("sdk-cfn-compression")
        .send()
        .await
        .unwrap();
    let stream = desc.delivery_stream_description().unwrap();
    assert_eq!(stream.version_id(), "2");
    let dest = stream.destinations()[0]
        .extended_s3_destination_description()
        .unwrap();
    assert_eq!(dest.compression_format().as_str(), "Snappy");
    assert_eq!(dest.prefix(), Some("before/"));
}

#[tokio::test]
async fn firehose_cfn_update_stack_rejects_invalid_retry() {
    firehose_cfn_invalid_retry(false).await;
}

#[tokio::test]
async fn firehose_cfn_execute_change_set_rejects_invalid_retry() {
    firehose_cfn_invalid_retry(true).await;
}

#[tokio::test]
async fn firehose_sdk_unknown_compression_rejected_without_mutation() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;
    let err = client
        .create_delivery_stream()
        .delivery_stream_name("invalid-compression")
        .extended_s3_destination_configuration(destination("BROTLI"))
        .send()
        .await
        .expect_err("SDK Unknown must reach server validation");
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("InvalidArgumentException")
    );
    assert!(client
        .list_delivery_streams()
        .send()
        .await
        .unwrap()
        .delivery_stream_names()
        .is_empty());

    client
        .create_delivery_stream()
        .delivery_stream_name("compression")
        .extended_s3_destination_configuration(destination("GZIP"))
        .send()
        .await
        .unwrap();
    let before = client
        .describe_delivery_stream()
        .delivery_stream_name("compression")
        .send()
        .await
        .unwrap();
    let desc = before.delivery_stream_description().unwrap();
    for update in [
        ExtendedS3DestinationUpdate::builder()
            .prefix("changed/")
            .compression_format(CompressionFormat::from("BROTLI"))
            .build(),
        ExtendedS3DestinationUpdate::builder()
            .prefix("changed/")
            .s3_backup_update(
                S3DestinationUpdate::builder()
                    .compression_format(CompressionFormat::from("BROTLI"))
                    .build(),
            )
            .build(),
    ] {
        let err = client
            .update_destination()
            .delivery_stream_name("compression")
            .current_delivery_stream_version_id(desc.version_id())
            .destination_id(desc.destinations()[0].destination_id())
            .extended_s3_destination_update(update)
            .send()
            .await
            .expect_err("invalid update must fail");
        assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("InvalidArgumentException")
        );
        let after = client
            .describe_delivery_stream()
            .delivery_stream_name("compression")
            .send()
            .await
            .unwrap();
        assert_eq!(
            after.delivery_stream_description(),
            before.delivery_stream_description()
        );
    }
}

#[tokio::test]
async fn firehose_sdk_all_valid_compression_values_round_trip() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("compression-bucket")
        .send()
        .await
        .unwrap();
    s3.put_bucket_versioning()
        .bucket("compression-bucket")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    for (format, updated_format) in [
        ("UNCOMPRESSED", "GZIP"),
        ("GZIP", "ZIP"),
        ("ZIP", "Snappy"),
        ("Snappy", "HADOOP_SNAPPY"),
        ("HADOOP_SNAPPY", "UNCOMPRESSED"),
    ] {
        let name = format!("compression-{format}");
        client
            .create_delivery_stream()
            .delivery_stream_name(&name)
            .extended_s3_destination_configuration(destination(format))
            .send()
            .await
            .unwrap();
        let before = client
            .describe_delivery_stream()
            .delivery_stream_name(&name)
            .send()
            .await
            .unwrap();
        let desc = before.delivery_stream_description().unwrap();
        assert_eq!(
            desc.destinations()[0]
                .extended_s3_destination_description()
                .unwrap()
                .compression_format()
                .as_str(),
            format
        );
        if ["ZIP", "HADOOP_SNAPPY"].contains(&format) {
            assert_unsupported_admission(&client, &name, format).await;
        }
        client
            .update_destination()
            .delivery_stream_name(&name)
            .current_delivery_stream_version_id(desc.version_id())
            .destination_id(desc.destinations()[0].destination_id())
            .extended_s3_destination_update(
                ExtendedS3DestinationUpdate::builder()
                    .compression_format(CompressionFormat::from(updated_format))
                    .prefix("after/")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let after = client
            .describe_delivery_stream()
            .delivery_stream_name(&name)
            .send()
            .await
            .unwrap();
        let desc = after.delivery_stream_description().unwrap();
        assert_eq!(desc.version_id(), "2");
        let s3 = desc.destinations()[0]
            .extended_s3_destination_description()
            .unwrap();
        assert_eq!(s3.compression_format().as_str(), updated_format);
        assert_eq!(s3.prefix(), Some("after/"));
        if ["ZIP", "HADOOP_SNAPPY"].contains(&updated_format) {
            assert_unsupported_admission(&client, &name, updated_format).await;
        }
    }
    let versions = s3
        .list_object_versions()
        .bucket("compression-bucket")
        .send()
        .await
        .unwrap();
    assert!(
        versions.versions().is_empty(),
        "rejected settings must never write fallback objects"
    );
    assert!(versions.delete_markers().is_empty());
}
