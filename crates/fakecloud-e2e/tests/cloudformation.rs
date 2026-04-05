mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn cloudformation_create_stack_with_sqs_queue() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let sqs_client = server.sqs_client().await;

    let template = r#"{
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cf-test-queue"
                }
            }
        }
    }"#;

    let result = cf_client
        .create_stack()
        .stack_name("test-sqs-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    assert!(result.stack_id().is_some());

    // Verify the queue was actually created
    let queues = sqs_client.list_queues().send().await.unwrap();
    let urls = queues.queue_urls();
    assert!(
        urls.iter().any(|u| u.contains("cf-test-queue")),
        "Queue cf-test-queue should exist, got: {urls:?}"
    );
}

#[tokio::test]
async fn cloudformation_create_stack_with_sns_topic_and_subscription() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let sns_client = server.sns_client().await;
    let sqs_client = server.sqs_client().await;

    let template = r#"{
        "Resources": {
            "MyTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {
                    "TopicName": "cf-test-topic"
                }
            },
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cf-sub-queue"
                }
            },
            "MySub": {
                "Type": "AWS::SNS::Subscription",
                "Properties": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:cf-test-topic",
                    "Protocol": "sqs",
                    "Endpoint": "arn:aws:sqs:us-east-1:123456789012:cf-sub-queue"
                }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("test-sns-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    // Verify topic was created
    let topics = sns_client.list_topics().send().await.unwrap();
    let topic_arns: Vec<String> = topics
        .topics()
        .iter()
        .filter_map(|t| t.topic_arn().map(|s| s.to_string()))
        .collect();
    assert!(
        topic_arns.iter().any(|a| a.contains("cf-test-topic")),
        "Topic cf-test-topic should exist, got: {topic_arns:?}"
    );

    // Verify queue was created
    let queues = sqs_client.list_queues().send().await.unwrap();
    assert!(queues
        .queue_urls()
        .iter()
        .any(|u| u.contains("cf-sub-queue")));

    // Verify subscription was created
    let subs = sns_client.list_subscriptions().send().await.unwrap();
    assert!(
        subs.subscriptions().iter().any(|s| {
            s.topic_arn()
                .map(|a| a.contains("cf-test-topic"))
                .unwrap_or(false)
        }),
        "Subscription should exist"
    );
}

#[tokio::test]
async fn cloudformation_delete_stack_removes_resources() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let sqs_client = server.sqs_client().await;

    let template = r#"{
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cf-delete-queue"
                }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("delete-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    // Verify queue exists
    let queues = sqs_client.list_queues().send().await.unwrap();
    assert!(queues
        .queue_urls()
        .iter()
        .any(|u| u.contains("cf-delete-queue")));

    // Delete the stack
    cf_client
        .delete_stack()
        .stack_name("delete-stack")
        .send()
        .await
        .unwrap();

    // Verify queue is gone
    let queues = sqs_client.list_queues().send().await.unwrap();
    assert!(
        !queues
            .queue_urls()
            .iter()
            .any(|u| u.contains("cf-delete-queue")),
        "Queue should be deleted after stack deletion"
    );
}

#[tokio::test]
async fn cloudformation_describe_stacks() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;

    let template = r#"{
        "Description": "Test stack description",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cf-describe-queue"
                }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("describe-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = cf_client
        .describe_stacks()
        .stack_name("describe-stack")
        .send()
        .await
        .unwrap();

    let stacks = result.stacks();
    assert_eq!(stacks.len(), 1);
    let stack = &stacks[0];
    assert_eq!(stack.stack_name(), Some("describe-stack"));
    assert_eq!(
        stack.stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE")
    );
}

#[tokio::test]
async fn cloudformation_list_stacks() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "Q1": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-list-q1" }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("list-stack-1")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let template2 = r#"{
        "Resources": {
            "Q2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-list-q2" }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("list-stack-2")
        .template_body(template2)
        .send()
        .await
        .unwrap();

    let result = cf_client.list_stacks().send().await.unwrap();
    let summaries = result.stack_summaries();
    assert!(summaries.len() >= 2);
}

#[tokio::test]
async fn cloudformation_list_stack_resources() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "Queue1": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-res-q1" }
            },
            "Queue2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-res-q2" }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("resources-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = cf_client
        .list_stack_resources()
        .stack_name("resources-stack")
        .send()
        .await
        .unwrap();

    let summaries = result.stack_resource_summaries();
    assert_eq!(summaries.len(), 2);

    let logical_ids: Vec<&str> = summaries
        .iter()
        .filter_map(|r| r.logical_resource_id())
        .collect();
    assert!(logical_ids.contains(&"Queue1"));
    assert!(logical_ids.contains(&"Queue2"));
}

#[tokio::test]
async fn cloudformation_create_stack_with_s3_bucket() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let s3_client = server.s3_client().await;

    let template = r#"{
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": "cf-test-bucket"
                }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("s3-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    // Verify the bucket was created
    let buckets = s3_client.list_buckets().send().await.unwrap();
    let bucket_names: Vec<&str> = buckets.buckets().iter().filter_map(|b| b.name()).collect();
    assert!(
        bucket_names.contains(&"cf-test-bucket"),
        "Bucket cf-test-bucket should exist, got: {bucket_names:?}"
    );
}

#[tokio::test]
async fn cloudformation_describe_stack_resources() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-dsr-queue" }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("dsr-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = cf_client
        .describe_stack_resources()
        .stack_name("dsr-stack")
        .send()
        .await
        .unwrap();

    let resources = result.stack_resources();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].logical_resource_id(), Some("MyQueue"));
    assert_eq!(resources[0].resource_type(), Some("AWS::SQS::Queue"));
}

#[tokio::test]
async fn cloudformation_get_template() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;

    let template = r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cf-gt-queue"}}}}"#;

    cf_client
        .create_stack()
        .stack_name("gt-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = cf_client
        .get_template()
        .stack_name("gt-stack")
        .send()
        .await
        .unwrap();

    let body = result.template_body().unwrap();
    assert!(body.contains("AWS::SQS::Queue"));
    assert!(body.contains("cf-gt-queue"));
}

#[tokio::test]
async fn cloudformation_update_stack() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let sqs_client = server.sqs_client().await;

    let template_v1 = r#"{
        "Resources": {
            "Queue1": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-update-q1" }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("update-stack")
        .template_body(template_v1)
        .send()
        .await
        .unwrap();

    // Update: remove Queue1, add Queue2
    let template_v2 = r#"{
        "Resources": {
            "Queue2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-update-q2" }
            }
        }
    }"#;

    cf_client
        .update_stack()
        .stack_name("update-stack")
        .template_body(template_v2)
        .send()
        .await
        .unwrap();

    // Verify Queue1 is gone and Queue2 exists
    let queues = sqs_client.list_queues().send().await.unwrap();
    let urls = queues.queue_urls();
    assert!(
        !urls.iter().any(|u| u.contains("cf-update-q1")),
        "Queue1 should be removed after update"
    );
    assert!(
        urls.iter().any(|u| u.contains("cf-update-q2")),
        "Queue2 should exist after update"
    );

    // Verify status is UPDATE_COMPLETE
    let desc = cf_client
        .describe_stacks()
        .stack_name("update-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.stacks()[0].stack_status().map(|s| s.as_str()),
        Some("UPDATE_COMPLETE")
    );
}
