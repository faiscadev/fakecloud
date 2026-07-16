//! Regression coverage for CloudFormation provisioner arms that accepted a
//! resource but dropped the properties that make it functional:
//!
//! - `AWS::IAM::Role` dropped `Policies` / `ManagedPolicyArns` (the role
//!   granted nothing under `--iam` enforcement).
//! - `AWS::Events::Rule` dropped `Targets` (the rule matched events but
//!   delivered to nothing).
//! - `AWS::DynamoDB::Table` dropped GSIs/LSIs (a Query on the index 400s at
//!   runtime).
//! - `AWS::SQS::Queue` dropped the typed `RedrivePolicy` and AWS-default
//!   attributes (DLQ routing never happened; drift vs an API-created queue).
//! - `AWS::SNS::Topic` dropped its inline `Subscription` list and config
//!   attributes (fan-out silently broken).
//!
//! Each resource is provisioned via CloudFormation, then read back via the
//! matching service SDK to assert the property survived.

mod helpers;

use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Role": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "cfn-props-role",
        "AssumeRolePolicyDocument": {
          "Version": "2012-10-17",
          "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
        },
        "ManagedPolicyArns": ["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"],
        "Policies": [{
          "PolicyName": "inline-logs",
          "PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "logs:PutLogEvents", "Resource": "*"}]}
        }]
      }
    },
    "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": "cfn-props-bus"}},
    "TargetQueue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-props-target"}},
    "Rule": {
      "Type": "AWS::Events::Rule",
      "Properties": {
        "Name": "cfn-props-rule",
        "EventBusName": "cfn-props-bus",
        "EventPattern": {"source": ["my.app"]},
        "Targets": [{"Id": "t1", "Arn": {"Fn::GetAtt": ["TargetQueue", "Arn"]}}]
      }
    },
    "Table": {
      "Type": "AWS::DynamoDB::Table",
      "Properties": {
        "TableName": "cfn-props-table",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
          {"AttributeName": "pk", "AttributeType": "S"},
          {"AttributeName": "gsi_pk", "AttributeType": "S"}
        ],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{
          "IndexName": "by-gsi",
          "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
          "Projection": {"ProjectionType": "ALL"}
        }]
      }
    },
    "DlqTarget": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-props-dlq"}},
    "MainQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": "cfn-props-main",
        "RedrivePolicy": {"deadLetterTargetArn": {"Fn::GetAtt": ["DlqTarget", "Arn"]}, "maxReceiveCount": 3}
      }
    },
    "SubEndpoint": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-props-sns-sub"}},
    "Topic": {
      "Type": "AWS::SNS::Topic",
      "Properties": {
        "TopicName": "cfn-props-topic",
        "DisplayName": "CFN Props Topic",
        "Subscription": [{"Protocol": "sqs", "Endpoint": {"Fn::GetAtt": ["SubEndpoint", "Arn"]}}]
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_provisioner_carries_resource_properties() {
    let server = TestServer::start().await;

    server
        .cloudformation_client()
        .await
        .create_stack()
        .stack_name("props")
        .template_body(TEMPLATE)
        .capabilities(aws_sdk_cloudformation::types::Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("create_stack");

    // --- IAM Role carries managed + inline policies ---
    let iam = server.iam_client().await;
    let attached = iam
        .list_attached_role_policies()
        .role_name("cfn-props-role")
        .send()
        .await
        .expect("list_attached_role_policies");
    assert!(
        attached
            .attached_policies()
            .iter()
            .any(|p| p.policy_arn() == Some("arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")),
        "managed policy not attached: {:?}",
        attached.attached_policies()
    );
    let inline = iam
        .list_role_policies()
        .role_name("cfn-props-role")
        .send()
        .await
        .expect("list_role_policies");
    assert!(
        inline.policy_names().iter().any(|n| n == "inline-logs"),
        "inline policy missing: {:?}",
        inline.policy_names()
    );

    // --- Events::Rule carries its target ---
    let events = server.eventbridge_client().await;
    let targets = events
        .list_targets_by_rule()
        .rule("cfn-props-rule")
        .event_bus_name("cfn-props-bus")
        .send()
        .await
        .expect("list_targets_by_rule");
    assert_eq!(
        targets.targets().len(),
        1,
        "rule target dropped: {:?}",
        targets.targets()
    );
    assert_eq!(targets.targets()[0].id(), "t1");

    // --- DynamoDB Table carries its GSI ---
    let ddb = server.dynamodb_client().await;
    let table = ddb
        .describe_table()
        .table_name("cfn-props-table")
        .send()
        .await
        .expect("describe_table");
    let gsis = table.table().unwrap().global_secondary_indexes();
    assert_eq!(gsis.len(), 1, "GSI dropped: {gsis:?}");
    assert_eq!(gsis[0].index_name(), Some("by-gsi"));

    // --- SQS Queue carries RedrivePolicy + AWS defaults ---
    let sqs = server.sqs_client().await;
    let url = sqs
        .get_queue_url()
        .queue_name("cfn-props-main")
        .send()
        .await
        .expect("get_queue_url")
        .queue_url()
        .unwrap()
        .to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::All)
        .send()
        .await
        .expect("get_queue_attributes");
    let map = attrs.attributes().unwrap();
    let redrive = map
        .get(&aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy)
        .expect("RedrivePolicy present");
    assert!(
        redrive.contains("cfn-props-dlq"),
        "RedrivePolicy lost DLQ target: {redrive}"
    );
    assert_eq!(
        map.get(&aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
            .map(String::as_str),
        Some("30"),
        "AWS default attributes not seeded"
    );

    // --- SNS Topic carries its inline subscription ---
    let sns = server.sns_client().await;
    let topics = sns.list_topics().send().await.expect("list_topics");
    let topic_arn = topics
        .topics()
        .iter()
        .filter_map(|t| t.topic_arn())
        .find(|a| a.ends_with(":cfn-props-topic"))
        .expect("cfn-props-topic exists")
        .to_string();
    let subs = sns
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("list_subscriptions_by_topic");
    assert_eq!(
        subs.subscriptions().len(),
        1,
        "inline SNS subscription dropped: {:?}",
        subs.subscriptions()
    );
    assert_eq!(subs.subscriptions()[0].protocol(), Some("sqs"));
}

/// `AWS::S3::Bucket` used to be provisioned from `BucketName` alone, dropping
/// every config property. Provision a fully-configured bucket (plus a tagged
/// SQS queue) and read each setting back through the matching S3/SQS API.
const S3_TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "NotifyQueue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-s3-notify"}},
    "TaggedQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": "cfn-s3-tagged",
        "Tags": [{"Key": "team", "Value": "core"}, {"Key": "env", "Value": "prod"}]
      }
    },
    "Bucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {
        "BucketName": "cfn-props-bucket",
        "VersioningConfiguration": {"Status": "Enabled"},
        "BucketEncryption": {
          "ServerSideEncryptionConfiguration": [{
            "BucketKeyEnabled": true,
            "ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}
          }]
        },
        "PublicAccessBlockConfiguration": {
          "BlockPublicAcls": true,
          "BlockPublicPolicy": true,
          "IgnorePublicAcls": true,
          "RestrictPublicBuckets": true
        },
        "NotificationConfiguration": {
          "QueueConfigurations": [{
            "Event": "s3:ObjectCreated:*",
            "Queue": {"Fn::GetAtt": ["NotifyQueue", "Arn"]}
          }]
        },
        "Tags": [{"Key": "owner", "Value": "platform"}]
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_s3_bucket_carries_config_and_sqs_tags() {
    let server = TestServer::start().await;

    server
        .cloudformation_client()
        .await
        .create_stack()
        .stack_name("s3-props")
        .template_body(S3_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let s3 = server.s3_client().await;

    // GetBucketVersioning
    let ver = s3
        .get_bucket_versioning()
        .bucket("cfn-props-bucket")
        .send()
        .await
        .expect("get_bucket_versioning");
    assert_eq!(
        ver.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled),
        "versioning not applied from CFN"
    );

    // GetBucketEncryption
    let enc = s3
        .get_bucket_encryption()
        .bucket("cfn-props-bucket")
        .send()
        .await
        .expect("get_bucket_encryption");
    let rules = enc
        .server_side_encryption_configuration()
        .expect("encryption config present")
        .rules();
    assert_eq!(rules.len(), 1, "encryption rule dropped");
    assert_eq!(
        rules[0]
            .apply_server_side_encryption_by_default()
            .map(|d| d.sse_algorithm()),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // GetPublicAccessBlock
    let pab = s3
        .get_public_access_block()
        .bucket("cfn-props-bucket")
        .send()
        .await
        .expect("get_public_access_block");
    let cfg = pab
        .public_access_block_configuration()
        .expect("pab present");
    assert_eq!(cfg.block_public_acls(), Some(true));
    assert_eq!(cfg.restrict_public_buckets(), Some(true));

    // GetBucketNotificationConfiguration
    let notif = s3
        .get_bucket_notification_configuration()
        .bucket("cfn-props-bucket")
        .send()
        .await
        .expect("get_bucket_notification_configuration");
    assert_eq!(
        notif.queue_configurations().len(),
        1,
        "notification queue config dropped"
    );
    assert!(notif.queue_configurations()[0]
        .queue_arn()
        .ends_with(":cfn-s3-notify"));

    // GetBucketTagging
    let tagging = s3
        .get_bucket_tagging()
        .bucket("cfn-props-bucket")
        .send()
        .await
        .expect("get_bucket_tagging");
    assert!(
        tagging
            .tag_set()
            .iter()
            .any(|t| t.key() == "owner" && t.value() == "platform"),
        "bucket tags dropped: {:?}",
        tagging.tag_set()
    );

    // SQS ListQueueTags on the CFN-created queue must be non-empty.
    let sqs = server.sqs_client().await;
    let url = sqs
        .get_queue_url()
        .queue_name("cfn-s3-tagged")
        .send()
        .await
        .expect("get_queue_url")
        .queue_url()
        .unwrap()
        .to_string();
    let tags = sqs
        .list_queue_tags()
        .queue_url(&url)
        .send()
        .await
        .expect("list_queue_tags");
    let map = tags.tags().expect("queue tags present");
    assert_eq!(map.get("team").map(String::as_str), Some("core"));
    assert_eq!(map.get("env").map(String::as_str), Some("prod"));
}

/// A CFN stack UPDATE that turns on versioning must be applied, not silently
/// dropped (there was no `AWS::S3::Bucket` arm in `update_resource`).
#[tokio::test]
async fn cfn_s3_bucket_update_enables_versioning() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;

    let v1 = r#"{
      "Resources": {
        "Bucket": {
          "Type": "AWS::S3::Bucket",
          "Properties": {"BucketName": "cfn-update-versioning"}
        }
      }
    }"#;
    cf.create_stack()
        .stack_name("s3-update")
        .template_body(v1)
        .send()
        .await
        .expect("create_stack");

    let s3 = server.s3_client().await;
    let before = s3
        .get_bucket_versioning()
        .bucket("cfn-update-versioning")
        .send()
        .await
        .expect("get_bucket_versioning v1");
    assert_ne!(
        before.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled),
        "versioning should be off before the update"
    );

    let v2 = r#"{
      "Resources": {
        "Bucket": {
          "Type": "AWS::S3::Bucket",
          "Properties": {
            "BucketName": "cfn-update-versioning",
            "VersioningConfiguration": {"Status": "Enabled"}
          }
        }
      }
    }"#;
    cf.update_stack()
        .stack_name("s3-update")
        .template_body(v2)
        .send()
        .await
        .expect("update_stack");

    let after = s3
        .get_bucket_versioning()
        .bucket("cfn-update-versioning")
        .send()
        .await
        .expect("get_bucket_versioning v2");
    assert_eq!(
        after.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled),
        "versioning update was a silent no-op"
    );
}
