mod helpers;

use aws_sdk_cloudformation::types::ChangeSetType;
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

#[tokio::test]
async fn execute_change_set_adds_and_removes_resources() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;

    let initial_template = r#"{
        "Resources": {
            "QueueA": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-queue-a"}
            }
        }
    }"#;

    cf.create_stack()
        .stack_name("cs-stack")
        .template_body(initial_template)
        .send()
        .await
        .unwrap();

    // Verify the initial queue exists
    let queues = sqs.list_queues().send().await.unwrap();
    assert!(
        queues.queue_urls().iter().any(|u| u.contains("cs-queue-a")),
        "initial queue should exist after CreateStack: {:?}",
        queues.queue_urls()
    );

    // Change set: add a second queue, drop the first.
    let new_template = r#"{
        "Resources": {
            "QueueB": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-queue-b"}
            }
        }
    }"#;

    let cs = cf
        .create_change_set()
        .stack_name("cs-stack")
        .change_set_name("cs1")
        .template_body(new_template)
        .send()
        .await
        .unwrap();
    let cs_id = cs.id().unwrap().to_string();

    // DescribeChangeSet should reflect the diff: Add QueueB, Remove QueueA.
    let describe = cf
        .describe_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .unwrap();
    let changes = describe.changes();
    assert_eq!(changes.len(), 2, "expected 2 changes, got {changes:?}");
    let actions: Vec<String> = changes
        .iter()
        .filter_map(|c| c.resource_change())
        .map(|rc| {
            rc.action()
                .map(|a| a.as_str().to_string())
                .unwrap_or_default()
        })
        .collect();
    assert!(
        actions.contains(&"Add".to_string()),
        "expected Add action: {actions:?}"
    );
    assert!(
        actions.contains(&"Remove".to_string()),
        "expected Remove action: {actions:?}"
    );
    assert_eq!(
        describe.execution_status().map(|s| s.as_str()),
        Some("AVAILABLE"),
    );

    // Execute and verify state
    cf.execute_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .unwrap();

    let queues = sqs.list_queues().send().await.unwrap();
    let urls: Vec<&str> = queues.queue_urls().iter().map(|s| s.as_str()).collect();
    assert!(
        urls.iter().any(|u| u.contains("cs-queue-b")),
        "queue-b should be created, got: {urls:?}"
    );
    assert!(
        !urls.iter().any(|u| u.contains("cs-queue-a")),
        "queue-a should be removed, got: {urls:?}"
    );

    // Stack status should be UPDATE_COMPLETE
    let stacks = cf
        .describe_stacks()
        .stack_name("cs-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stacks.stacks()[0]
            .stack_status()
            .map(|s| s.as_str().to_string()),
        Some("UPDATE_COMPLETE".to_string()),
    );

    // ChangeSet ExecutionStatus should be EXECUTE_COMPLETE
    let post = cf
        .describe_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        post.execution_status().map(|s| s.as_str()),
        Some("EXECUTE_COMPLETE"),
    );
}

#[tokio::test]
async fn execute_change_set_modify_only() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;
    let sns = server.sns_client().await;

    let template_v1 = r#"{
        "Resources": {
            "Topic1": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": "cs-mod-topic"}
            }
        }
    }"#;

    cf.create_stack()
        .stack_name("cs-mod-stack")
        .template_body(template_v1)
        .send()
        .await
        .unwrap();

    // Same template, different DisplayName so the diff is Modify.
    let template_v2 = r#"{
        "Resources": {
            "Topic1": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": "cs-mod-topic", "DisplayName": "v2"}
            }
        }
    }"#;

    let cs = cf
        .create_change_set()
        .stack_name("cs-mod-stack")
        .change_set_name("modify-cs")
        .template_body(template_v2)
        .send()
        .await
        .unwrap();
    let cs_id = cs.id().unwrap().to_string();

    let describe = cf
        .describe_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .unwrap();
    let changes = describe.changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0]
            .resource_change()
            .and_then(|rc| rc.action())
            .map(|a| a.as_str().to_string()),
        Some("Modify".to_string()),
    );

    cf.execute_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .unwrap();

    // Topic still exists
    let topics = sns.list_topics().send().await.unwrap();
    assert!(topics
        .topics()
        .iter()
        .any(|t| t.topic_arn().is_some_and(|a| a.contains("cs-mod-topic"))));

    let stacks = cf
        .describe_stacks()
        .stack_name("cs-mod-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stacks.stacks()[0]
            .stack_status()
            .map(|s| s.as_str().to_string()),
        Some("UPDATE_COMPLETE".to_string()),
    );
}

#[tokio::test]
async fn execute_change_set_emits_stack_events_and_lists_resources() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;

    // Stack starts with one queue.
    let template_v1 = r#"{
        "Resources": {
            "QueueOne": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-events-q1"}
            }
        }
    }"#;
    cf.create_stack()
        .stack_name("cs-events-stack")
        .template_body(template_v1)
        .send()
        .await
        .unwrap();

    // Change set adds a second queue (preserves the first).
    let template_v2 = r#"{
        "Resources": {
            "QueueOne": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-events-q1"}
            },
            "QueueTwo": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-events-q2"}
            }
        }
    }"#;
    let cs = cf
        .create_change_set()
        .stack_name("cs-events-stack")
        .change_set_name("add-q2")
        .template_body(template_v2)
        .send()
        .await
        .unwrap();
    let cs_id = cs.id().unwrap().to_string();

    cf.execute_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .unwrap();

    // Stack now reports UPDATE_COMPLETE.
    let stacks = cf
        .describe_stacks()
        .stack_name("cs-events-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stacks.stacks()[0]
            .stack_status()
            .map(|s| s.as_str().to_string()),
        Some("UPDATE_COMPLETE".to_string()),
    );

    // ListStackResources should contain both logical IDs.
    let listed = cf
        .list_stack_resources()
        .stack_name("cs-events-stack")
        .send()
        .await
        .unwrap();
    let ids: Vec<String> = listed
        .stack_resource_summaries()
        .iter()
        .map(|r| r.logical_resource_id().unwrap_or_default().to_string())
        .collect();
    assert!(ids.contains(&"QueueOne".to_string()), "ids={ids:?}");
    assert!(ids.contains(&"QueueTwo".to_string()), "ids={ids:?}");

    // DescribeStackEvents should reflect the lifecycle of the change-set
    // execution: an UPDATE_IN_PROGRESS / UPDATE_COMPLETE pair on the stack
    // itself plus a CREATE_IN_PROGRESS / CREATE_COMPLETE pair for the new
    // resource (`QueueTwo`). The resource that already existed
    // (`QueueOne`) does not emit new events because there's nothing to
    // update.
    let events = cf
        .describe_stack_events()
        .stack_name("cs-events-stack")
        .send()
        .await
        .unwrap();
    let rows: Vec<(String, String)> = events
        .stack_events()
        .iter()
        .map(|e| {
            (
                e.logical_resource_id().unwrap_or_default().to_string(),
                e.resource_status()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
            )
        })
        .collect();
    assert!(
        rows.iter()
            .any(|(l, s)| l == "QueueTwo" && s == "CREATE_COMPLETE"),
        "expected CREATE_COMPLETE for QueueTwo, got {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|(l, s)| l == "cs-events-stack" && s == "UPDATE_COMPLETE"),
        "expected stack UPDATE_COMPLETE, got {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|(l, s)| l == "cs-events-stack" && s == "UPDATE_IN_PROGRESS"),
        "expected stack UPDATE_IN_PROGRESS, got {rows:?}"
    );
}

/// `aws cloudformation deploy`, SAM, and CDK create first-time stacks
/// through a `ChangeSetType=CREATE` change set, not raw CreateStack. The
/// stack must materialize in `REVIEW_IN_PROGRESS` and be created on
/// execute (issue #1646, gap 1). DescribeStacks on the not-yet-created
/// stack must return an error so the tools' existence probe works (gap 3).
#[tokio::test]
async fn create_type_change_set_creates_stack_on_execute() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;

    // Gap 3: probing a stack that doesn't exist yet must error, not
    // return an empty list (SAM/`deploy` catch this to branch create-vs-update).
    let probe = cf
        .describe_stacks()
        .stack_name("cs-create-stack")
        .send()
        .await;
    assert!(
        probe.is_err(),
        "DescribeStacks on a missing stack must error, got {probe:?}"
    );

    let template = r#"{
        "Resources": {
            "NewQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-create-queue"}
            }
        }
    }"#;

    cf.create_change_set()
        .stack_name("cs-create-stack")
        .change_set_name("create-cs")
        .change_set_type(ChangeSetType::Create)
        .template_body(template)
        .send()
        .await
        .unwrap();

    // The stack now exists in REVIEW_IN_PROGRESS (no resources yet).
    let review = cf
        .describe_stacks()
        .stack_name("cs-create-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        review.stacks()[0].stack_status().map(|s| s.as_str()),
        Some("REVIEW_IN_PROGRESS"),
    );
    assert!(
        !sqs.list_queues()
            .send()
            .await
            .unwrap()
            .queue_urls()
            .iter()
            .any(|u| u.contains("cs-create-queue")),
        "queue must not exist before the change set executes"
    );

    cf.execute_change_set()
        .stack_name("cs-create-stack")
        .change_set_name("create-cs")
        .send()
        .await
        .unwrap();

    // Stack created; the queue is provisioned.
    let created = cf
        .describe_stacks()
        .stack_name("cs-create-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        created.stacks()[0].stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE"),
    );
    assert!(
        sqs.list_queues()
            .send()
            .await
            .unwrap()
            .queue_urls()
            .iter()
            .any(|u| u.contains("cs-create-queue")),
        "queue should be created after executing the CREATE change set"
    );
}

/// SAM always — and `aws cloudformation deploy`/CDK for large templates —
/// pass the template by `TemplateURL` pointing at an object in S3. The
/// change set must fetch it (issue #1646, gap 2) rather than store an
/// empty template and silently no-op on execute.
#[tokio::test]
async fn create_change_set_resolves_template_url() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;
    let s3 = server.s3_client().await;
    let sns = server.sns_client().await;

    let template = r#"{
        "Resources": {
            "UrlTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": "cs-url-topic"}
            }
        }
    }"#;

    s3.create_bucket()
        .bucket("cfn-templates")
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("cfn-templates")
        .key("deploy/template.json")
        .body(ByteStream::from(template.as_bytes().to_vec()))
        .send()
        .await
        .unwrap();

    let template_url = format!("{}/cfn-templates/deploy/template.json", server.endpoint());

    cf.create_change_set()
        .stack_name("cs-url-stack")
        .change_set_name("url-cs")
        .change_set_type(ChangeSetType::Create)
        .template_url(&template_url)
        .send()
        .await
        .unwrap();

    cf.execute_change_set()
        .stack_name("cs-url-stack")
        .change_set_name("url-cs")
        .send()
        .await
        .unwrap();

    let stacks = cf
        .describe_stacks()
        .stack_name("cs-url-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stacks.stacks()[0].stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE"),
    );
    assert!(
        sns.list_topics()
            .send()
            .await
            .unwrap()
            .topics()
            .iter()
            .any(|t| t.topic_arn().is_some_and(|a| a.contains("cs-url-topic"))),
        "the TemplateURL-sourced topic should be provisioned"
    );
}

/// A `ChangeSetType=CREATE` change set against a stack that already exists
/// must be rejected (AWS: AlreadyExistsException), not silently executed
/// as an UPDATE.
#[tokio::test]
async fn create_type_change_set_rejected_for_existing_stack() {
    let server = TestServer::start().await;
    let cf = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cs-exists-q"}}
        }
    }"#;

    cf.create_stack()
        .stack_name("cs-exists-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let err = cf
        .create_change_set()
        .stack_name("cs-exists-stack")
        .change_set_name("dup-create")
        .change_set_type(ChangeSetType::Create)
        .template_body(template)
        .send()
        .await;
    assert!(
        err.is_err(),
        "CREATE change set against an existing stack must be rejected, got {err:?}"
    );
}
