//! CloudFormation provisioners for resource-policy types:
//! AWS::SQS::QueuePolicy, AWS::SNS::TopicPolicy, AWS::S3::BucketPolicy.
//! Each stores the PolicyDocument on its parent resource so the parent
//! service's Get API round-trips it.

mod helpers;

use aws_sdk_sqs::types::QueueAttributeName;
use helpers::TestServer;

fn output<'a>(stack: &'a aws_sdk_cloudformation::types::Stack, key: &str) -> Option<&'a str> {
    stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
}

const QUEUE_POLICY_TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-qp-queue"}},
    "QP": {
      "Type": "AWS::SQS::QueuePolicy",
      "Properties": {
        "Queues": [{"Ref": "Q"}],
        "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
          "Effect": "Allow",
          "Principal": {"Service": "sns.amazonaws.com"},
          "Action": "sqs:SendMessage",
          "Resource": "*"
        }]}
      }
    }
  },
  "Outputs": {"QueueUrl": {"Value": {"Ref": "Q"}}}
}"#;

#[tokio::test]
async fn cfn_provisions_sqs_queue_policy() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;

    cfn.create_stack()
        .stack_name("qp-stack")
        .template_body(QUEUE_POLICY_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("qp-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");
    let queue_url = output(stack, "QueueUrl")
        .expect("QueueUrl output")
        .to_string();

    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::Policy)
        .send()
        .await
        .expect("get_queue_attributes");
    let policy = attrs
        .attributes()
        .and_then(|m| m.get(&QueueAttributeName::Policy))
        .expect("Policy attribute present");
    assert!(
        policy.contains("sqs:SendMessage"),
        "queue policy should round-trip: {policy}"
    );

    cfn.delete_stack()
        .stack_name("qp-stack")
        .send()
        .await
        .expect("delete_stack");
    let after = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .send()
        .await;
    assert!(after.is_err(), "queue should be gone after stack deletion");
}

const TOPIC_POLICY_TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "T": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "cfn-tp-topic"}},
    "TP": {
      "Type": "AWS::SNS::TopicPolicy",
      "Properties": {
        "Topics": [{"Ref": "T"}],
        "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
          "Effect": "Allow",
          "Principal": {"Service": "events.amazonaws.com"},
          "Action": "sns:Publish",
          "Resource": "*"
        }]}
      }
    }
  },
  "Outputs": {"TopicArn": {"Value": {"Ref": "T"}}}
}"#;

#[tokio::test]
async fn cfn_provisions_sns_topic_policy() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sns = server.sns_client().await;

    cfn.create_stack()
        .stack_name("tp-stack")
        .template_body(TOPIC_POLICY_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("tp-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");
    let topic_arn = output(stack, "TopicArn")
        .expect("TopicArn output")
        .to_string();

    let attrs = sns
        .get_topic_attributes()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("get_topic_attributes");
    let policy = attrs
        .attributes()
        .and_then(|m| m.get("Policy"))
        .expect("Policy attribute present");
    assert!(
        policy.contains("sns:Publish"),
        "topic policy should round-trip: {policy}"
    );

    cfn.delete_stack()
        .stack_name("tp-stack")
        .send()
        .await
        .expect("delete_stack");
}

const BUCKET_POLICY_TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "B": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "cfn-bp-bucket"}},
    "BP": {
      "Type": "AWS::S3::BucketPolicy",
      "Properties": {
        "Bucket": {"Ref": "B"},
        "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
          "Effect": "Allow",
          "Principal": "*",
          "Action": "s3:GetObject",
          "Resource": "arn:aws:s3:::cfn-bp-bucket/*"
        }]}
      }
    }
  },
  "Outputs": {"BucketName": {"Value": {"Ref": "B"}}}
}"#;

#[tokio::test]
async fn cfn_provisions_s3_bucket_policy() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let s3 = server.s3_client().await;

    cfn.create_stack()
        .stack_name("bp-stack")
        .template_body(BUCKET_POLICY_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("bp-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");
    let bucket = output(stack, "BucketName")
        .expect("BucketName output")
        .to_string();

    let policy = s3
        .get_bucket_policy()
        .bucket(&bucket)
        .send()
        .await
        .expect("get_bucket_policy");
    assert!(
        policy
            .policy()
            .map(|s| s.contains("s3:GetObject"))
            .unwrap_or(false),
        "bucket policy should round-trip"
    );

    cfn.delete_stack()
        .stack_name("bp-stack")
        .send()
        .await
        .expect("delete_stack");
    let after = s3.get_bucket_policy().bucket(&bucket).send().await;
    assert!(
        after.is_err(),
        "bucket policy should be gone after deletion"
    );
}
