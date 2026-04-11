mod helpers;

use std::io::Write;

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

/// Create a ZIP file in memory containing a single file.
fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content).unwrap();
    }
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

async fn get_lambda_invocations(endpoint: &str) -> serde_json::Value {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{endpoint}/_fakecloud/lambda/invocations"))
        .send()
        .await
        .unwrap();
    resp.json::<serde_json::Value>().await.unwrap()
}

#[tokio::test]
async fn cloudformation_custom_resource_invokes_lambda() {
    use aws_sdk_lambda::primitives::Blob;
    use aws_sdk_lambda::types::{FunctionCode, Runtime};

    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let lambda_client = server.lambda_client().await;

    // 1. Create a simple no-op Lambda
    let python_code = r#"
def lambda_handler(event, context):
    return {"statusCode": 200, "body": "ok"}
"#;

    let zip = make_zip(&[("lambda_function.py", python_code.as_bytes())]);

    let function_name = "cf-custom-handler";
    lambda_client
        .create_function()
        .function_name(function_name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("lambda_function.lambda_handler")
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    let lambda_arn = format!(
        "arn:aws:lambda:us-east-1:123456789012:function:{}",
        function_name
    );

    // 2. Create a CF stack with a Custom::MyResource
    let template = serde_json::json!({
        "Resources": {
            "MyCustom": {
                "Type": "Custom::MyResource",
                "Properties": {
                    "ServiceToken": lambda_arn,
                    "Foo": "bar",
                    "Count": 42
                }
            }
        }
    });

    let result = cf_client
        .create_stack()
        .stack_name("custom-resource-stack")
        .template_body(template.to_string())
        .send()
        .await
        .unwrap();
    assert!(result.stack_id().is_some());

    // 3. Assert CloudFormation invoked the Lambda with the correct custom resource event
    let mut invocation_payload = None;
    for _ in 0..10 {
        let invocations = get_lambda_invocations(server.endpoint()).await;
        if let Some(inv_list) = invocations["invocations"].as_array() {
            if let Some(inv) = inv_list.iter().find(|inv| {
                inv["functionArn"] == lambda_arn && inv["source"] == "aws:lambda:delivery"
            }) {
                invocation_payload = Some(inv["payload"].clone());
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    let invocation_payload = invocation_payload.expect(
        "CloudFormation should have recorded a Lambda delivery invocation for the custom resource",
    );

    let invocation_event = if let Some(payload_str) = invocation_payload.as_str() {
        serde_json::from_str::<serde_json::Value>(payload_str).unwrap()
    } else {
        invocation_payload
    };

    assert_eq!(invocation_event["RequestType"], "Create");
    assert_eq!(invocation_event["ResourceType"], "Custom::MyResource");
    assert_eq!(invocation_event["LogicalResourceId"], "MyCustom");
    assert_eq!(invocation_event["ResourceProperties"]["Foo"], "bar");
    assert_eq!(invocation_event["ResourceProperties"]["Count"], 42);
}

#[tokio::test]
async fn cloudformation_stack_sends_sns_notification() {
    let server = TestServer::start().await;
    let cf_client = server.cloudformation_client().await;
    let sns_client = server.sns_client().await;
    let sqs_client = server.sqs_client().await;

    // Create an SNS topic for stack notifications
    let topic = sns_client
        .create_topic()
        .name("cf-notifications")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Create an SQS queue to receive SNS messages
    let queue = sqs_client
        .create_queue()
        .queue_name("cf-notif-receiver")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_attrs = sqs_client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = queue_attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Subscribe SQS to SNS
    sns_client
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();

    // Create a stack with NotificationARNs
    let template = r#"{
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cf-notif-test-queue"
                }
            }
        }
    }"#;

    cf_client
        .create_stack()
        .stack_name("notif-stack")
        .template_body(template)
        .notification_arns(&topic_arn)
        .send()
        .await
        .unwrap();

    // Check SQS for the notification
    let msgs = sqs_client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert!(
        !msgs.messages().is_empty(),
        "expected at least one CloudFormation notification in SQS"
    );

    let body = msgs.messages()[0].body().unwrap();
    // SNS wraps the message in a JSON envelope
    let envelope: serde_json::Value = serde_json::from_str(body).unwrap();
    let message = envelope["Message"].as_str().unwrap_or(body);
    assert!(
        message.contains("CREATE_COMPLETE"),
        "notification should contain CREATE_COMPLETE, got: {}",
        message
    );
    assert!(
        message.contains("notif-stack"),
        "notification should contain stack name"
    );
}
