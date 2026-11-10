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

async fn wait_stack_terminal(
    cfn: &aws_sdk_cloudformation::Client,
    stack: &str,
) -> (String, String) {
    helpers::wait_until(std::time::Duration::from_secs(60), || async {
        let out = cfn.describe_stacks().stack_name(stack).send().await.ok()?;
        let s = out.stacks().first()?;
        let status = s.stack_status()?.as_str().to_string();
        (!status.ends_with("IN_PROGRESS")).then(|| {
            (
                status,
                s.stack_status_reason().unwrap_or_default().to_string(),
            )
        })
    })
    .await
    .expect("stack reached a terminal status")
}

const RETAINED_TABLE: &str = r#"{
  "Resources": {
    "Table": {
      "Type": "AWS::DynamoDB::Table",
      "DeletionPolicy": "Retain",
      "Properties": {
        "TableName": "cfn-retained-table",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
      }
    }
  }
}"#;

/// `DeletionPolicy: Retain` + an explicit name: deleting the stack keeps the
/// table, and deploying the stack again fails with "already exists" instead
/// of overwriting the retained table (and wiping its items).
#[tokio::test]
async fn cfn_redeploy_over_retained_table_fails_and_keeps_data() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ddb = server.dynamodb_client().await;

    cfn.create_stack()
        .stack_name("retain-1")
        .template_body(RETAINED_TABLE)
        .send()
        .await
        .unwrap();
    assert_eq!(
        wait_stack_terminal(&cfn, "retain-1").await.0,
        "CREATE_COMPLETE"
    );
    ddb.put_item()
        .table_name("cfn-retained-table")
        .item(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("keep-me".into()),
        )
        .send()
        .await
        .unwrap();
    cfn.delete_stack()
        .stack_name("retain-1")
        .send()
        .await
        .unwrap();
    helpers::wait_until(std::time::Duration::from_secs(30), || async {
        let out = cfn.describe_stacks().stack_name("retain-1").send().await;
        match out {
            Err(_) => Some(()),
            Ok(o) => o
                .stacks()
                .first()
                .and_then(|s| s.stack_status())
                .filter(|s| s.as_str() == "DELETE_COMPLETE")
                .map(|_| ()),
        }
    })
    .await
    .expect("stack deleted");

    let result = cfn
        .create_stack()
        .stack_name("retain-2")
        .template_body(RETAINED_TABLE)
        .send()
        .await;
    let reason = match result {
        Err(e) => format!("{e:?}"),
        Ok(_) => {
            let (status, _) = wait_stack_terminal(&cfn, "retain-2").await;
            assert_ne!(status, "CREATE_COMPLETE");
            // A rolled-back stack keeps no reason; the failed resource's event does.
            let events = cfn
                .describe_stack_events()
                .stack_name("retain-2")
                .send()
                .await
                .unwrap();
            events
                .stack_events()
                .iter()
                .find(|e| e.resource_status().map(|s| s.as_str()) == Some("CREATE_FAILED"))
                .and_then(|e| e.resource_status_reason())
                .unwrap_or_default()
                .to_string()
        }
    };
    assert!(reason.contains("already exists"), "{reason}");

    let got = ddb
        .get_item()
        .table_name("cfn-retained-table")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("keep-me".into()),
        )
        .send()
        .await
        .unwrap();
    assert!(got.item().is_some(), "retained item survived the redeploy");
}

/// TimeToLiveSpecification, PointInTimeRecoverySpecification and
/// KinesisStreamSpecification reach the table (DescribeTimeToLive /
/// DescribeContinuousBackups / DescribeKinesisStreamingDestination), and a
/// stack update that turns on StreamSpecification yields a StreamArn.
#[tokio::test]
async fn cfn_dynamodb_table_applies_ttl_pitr_kinesis_and_stream_updates() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ddb = server.dynamodb_client().await;
    let template = |stream: bool| {
        let mut props = serde_json::json!({
            "TableName": "cfn-ttl-table",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "ProvisionedThroughput": {"ReadCapacityUnits": 2, "WriteCapacityUnits": 2},
            "TimeToLiveSpecification": {"AttributeName": "expires", "Enabled": true},
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": true},
            "KinesisStreamSpecification": {"StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/ttl-dest"}
        });
        if stream {
            props["StreamSpecification"] =
                serde_json::json!({"StreamViewType": "NEW_AND_OLD_IMAGES"});
        }
        serde_json::json!({
            "Resources": {"Table": {"Type": "AWS::DynamoDB::Table", "Properties": props}},
            "Outputs": {"StreamArn": {"Value": {"Fn::GetAtt": ["Table", "StreamArn"]}}}
        })
        .to_string()
    };
    let initial = serde_json::from_str::<serde_json::Value>(&template(false)).unwrap();
    let mut initial = initial;
    initial.as_object_mut().unwrap().remove("Outputs");
    cfn.create_stack()
        .stack_name("ddb-settings")
        .template_body(initial.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(
        wait_stack_terminal(&cfn, "ddb-settings").await.0,
        "CREATE_COMPLETE"
    );

    let table = ddb
        .describe_table()
        .table_name("cfn-ttl-table")
        .send()
        .await
        .unwrap();
    // CloudFormation's BillingMode default is PROVISIONED.
    let summary = table.table().unwrap().billing_mode_summary();
    assert!(
        summary.is_none_or(|s| s.billing_mode().map(|m| m.as_str()) == Some("PROVISIONED")),
        "{summary:?}"
    );
    let ttl = ddb
        .describe_time_to_live()
        .table_name("cfn-ttl-table")
        .send()
        .await
        .unwrap();
    let ttl = ttl.time_to_live_description().unwrap();
    assert_eq!(ttl.time_to_live_status().unwrap().as_str(), "ENABLED");
    assert_eq!(ttl.attribute_name(), Some("expires"));
    let backups = ddb
        .describe_continuous_backups()
        .table_name("cfn-ttl-table")
        .send()
        .await
        .unwrap();
    assert_eq!(
        backups
            .continuous_backups_description()
            .unwrap()
            .point_in_time_recovery_description()
            .unwrap()
            .point_in_time_recovery_status()
            .unwrap()
            .as_str(),
        "ENABLED"
    );
    let kinesis = ddb
        .describe_kinesis_streaming_destination()
        .table_name("cfn-ttl-table")
        .send()
        .await
        .unwrap();
    assert_eq!(
        kinesis.kinesis_data_stream_destinations()[0].stream_arn(),
        Some("arn:aws:kinesis:us-east-1:123456789012:stream/ttl-dest")
    );

    cfn.update_stack()
        .stack_name("ddb-settings")
        .template_body(template(true))
        .send()
        .await
        .unwrap();
    assert_eq!(
        wait_stack_terminal(&cfn, "ddb-settings").await.0,
        "UPDATE_COMPLETE"
    );
    let table = ddb
        .describe_table()
        .table_name("cfn-ttl-table")
        .send()
        .await
        .unwrap();
    let stream_arn = table
        .table()
        .unwrap()
        .latest_stream_arn()
        .expect("stream enabled by the update")
        .to_string();
    let outputs = cfn
        .describe_stacks()
        .stack_name("ddb-settings")
        .send()
        .await
        .unwrap()
        .stacks()[0]
        .outputs()
        .to_vec();
    assert_eq!(
        outputs
            .iter()
            .find(|o| o.output_key() == Some("StreamArn"))
            .and_then(|o| o.output_value()),
        Some(stream_arn.as_str())
    );
}
