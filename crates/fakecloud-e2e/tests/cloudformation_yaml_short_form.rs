//! CloudFormation's YAML dialect lets intrinsics be written as short-form node
//! tags (`!Ref`, `!GetAtt`, `!Sub`, ...). Plain YAML deserialization rejects
//! those tags and fails the *whole* document, so a template using them parsed
//! to nothing: CreateStack reported CREATE_COMPLETE with no resources, no
//! outputs and no resource-level events, and nothing was provisioned (#2480).
//!
//! The first test is the reported reproduction verbatim.

mod helpers;

use helpers::TestServer;

/// The template from the #2480 report: a versioned bucket whose name is
/// surfaced through a short-form `!Ref` output.
const REPRO_TEMPLATE: &str = r"
AWSTemplateFormatVersion: '2010-09-09'
Description: Minimal reproduction for FakeCloud CloudFormation provisioning

Resources:
  ReproBucket:
    Type: AWS::S3::Bucket
    Properties:
      VersioningConfiguration:
        Status: Enabled

Outputs:
  BucketName:
    Value: !Ref ReproBucket
";

#[tokio::test]
async fn short_form_ref_template_provisions_its_resources() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("cfn-control")
        .template_body(REPRO_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("cfn-control")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // The `!Ref` output resolves to the bucket's physical name.
    let bucket_name = stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some("BucketName"))
        .and_then(|o| o.output_value())
        .expect("BucketName output present");
    assert!(!bucket_name.is_empty(), "!Ref output must resolve");

    // The resource is recorded on the stack.
    let resources = cfn
        .describe_stack_resources()
        .stack_name("cfn-control")
        .send()
        .await
        .expect("describe_stack_resources");
    let bucket = resources
        .stack_resources()
        .iter()
        .find(|r| r.logical_resource_id() == Some("ReproBucket"))
        .expect("ReproBucket recorded as a stack resource");
    assert_eq!(bucket.resource_type(), Some("AWS::S3::Bucket"));
    assert_eq!(bucket.physical_resource_id(), Some(bucket_name));

    // ... and a resource-level event was emitted for it, not just the two
    // stack-level ones.
    let events = cfn
        .describe_stack_events()
        .stack_name("cfn-control")
        .send()
        .await
        .expect("describe_stack_events");
    assert!(
        events
            .stack_events()
            .iter()
            .any(|e| e.logical_resource_id() == Some("ReproBucket")),
        "expected a resource-level event for ReproBucket, got {:?}",
        events
            .stack_events()
            .iter()
            .map(|e| e.logical_resource_id())
            .collect::<Vec<_>>()
    );

    // The bucket really exists in S3, with the versioning the template asked
    // for.
    let s3 = server.s3_client().await;
    let buckets = s3.list_buckets().send().await.expect("list_buckets");
    assert!(
        buckets
            .buckets()
            .iter()
            .any(|b| b.name() == Some(bucket_name)),
        "CloudFormation-created bucket should be listed by S3"
    );

    let versioning = s3
        .get_bucket_versioning()
        .bucket(bucket_name)
        .send()
        .await
        .expect("get_bucket_versioning");
    assert_eq!(
        versioning.status().map(|s| s.as_str()),
        Some("Enabled"),
        "VersioningConfiguration from the template should be applied"
    );
}

/// `!GetAtt`, `!Sub`, `!Join` and `!Select` across two resources, so the
/// short forms are exercised where they carry real cross-resource resolution
/// rather than a single `Ref`.
const INTRINSICS_TEMPLATE: &str = r#"
AWSTemplateFormatVersion: '2010-09-09'
Parameters:
  Prefix:
    Type: String
    Default: shortform
Resources:
  Topic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "${Prefix}-topic"
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Join ["-", [!Ref Prefix, queue]]
Outputs:
  TopicArn:
    Value: !GetAtt Topic.TopicArn
  QueueName:
    Value: !Select [0, [!Ref Queue]]
"#;

#[tokio::test]
async fn short_form_intrinsics_resolve_across_resources() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("shortform-intrinsics")
        .template_body(INTRINSICS_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("shortform-intrinsics")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    let output = |key: &str| -> String {
        stack
            .outputs()
            .iter()
            .find(|o| o.output_key() == Some(key))
            .and_then(|o| o.output_value())
            .unwrap_or_else(|| panic!("{key} output present"))
            .to_string()
    };

    // `!Sub` interpolated the parameter into the topic name, and `!GetAtt`
    // reached the provisioned topic's ARN.
    let topic_arn = output("TopicArn");
    assert!(
        topic_arn.starts_with("arn:aws:sns:") && topic_arn.ends_with(":shortform-topic"),
        "unexpected TopicArn {topic_arn}"
    );

    // `!Join` built the queue name from the parameter, and `!Select` picked it
    // back out of a single-element list.
    let queue_name = output("QueueName");
    assert!(
        queue_name.ends_with("shortform-queue"),
        "unexpected QueueName {queue_name}"
    );

    let sqs = server.sqs_client().await;
    let queues = sqs.list_queues().send().await.expect("list_queues");
    assert!(
        queues
            .queue_urls()
            .iter()
            .any(|u| u.ends_with("/shortform-queue")),
        "the !Join-named queue should exist, got {:?}",
        queues.queue_urls()
    );
}

/// A template document that is genuinely broken (a resource with no `Type`)
/// must not silently report CREATE_COMPLETE with an empty stack — that is the
/// failure mode #2480 reported, and it hides real template bugs.
const BROKEN_TEMPLATE: &str = r"
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  NoTypeHere:
    Properties:
      Whatever: 1
";

#[tokio::test]
async fn unparseable_template_document_fails_the_stack() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("broken-template")
        .template_body(BROKEN_TEMPLATE)
        .send()
        .await
        .expect("create_stack returns a StackId; the failure is asynchronous");

    let described = cfn
        .describe_stacks()
        .stack_name("broken-template")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(
        stack.stack_status().unwrap().as_str(),
        "CREATE_FAILED",
        "a template that cannot be parsed must not report CREATE_COMPLETE"
    );
    let reason = stack.stack_status_reason().unwrap_or_default();
    assert!(
        reason.contains("NoTypeHere"),
        "the status reason should name the offending resource, got {reason:?}"
    );
}
