//! Stack update order and rollback, mirroring what CloudFormation was recorded
//! doing on a real account on 2026-11-09. Each test recreates one recorded
//! experiment, and asserts the same events and final state.
//!
//! AWS applies resources in parallel where fakecloud goes one at a time, so the
//! assertions fix each resource's own event sequence and the order of the phases
//! rather than how resources interleave.

mod helpers;

use aws_sdk_cloudformation::types::{ChangeSetType, StackEvent};
use aws_sdk_cloudformation::Client;
use helpers::TestServer;
use serde_json::{json, Value};

const STACK_TYPE: &str = "AWS::CloudFormation::Stack";

fn table(name: Option<&str>, key: &str) -> Value {
    let mut properties = json!({
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [{"AttributeName": key, "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}]
    });
    if let Some(name) = name {
        properties["TableName"] = json!(name);
    }
    json!({"Type": "AWS::DynamoDB::Table", "Properties": properties})
}

fn queue(visibility: u32) -> Value {
    json!({"Type": "AWS::SQS::Queue", "Properties": {"VisibilityTimeout": visibility}})
}

fn resources(entries: Value) -> String {
    json!({"Resources": entries}).to_string()
}

async fn create(cfn: &Client, stack: &str, template: &str) -> String {
    let out = cfn
        .create_stack()
        .stack_name(stack)
        .template_body(template)
        .send()
        .await
        .expect("create_stack");
    let stack_id = out.stack_id().unwrap().to_string();
    assert_eq!(status(cfn, stack).await, "CREATE_COMPLETE");
    stack_id
}

async fn update(cfn: &Client, stack: &str, template: &str) {
    cfn.update_stack()
        .stack_name(stack)
        .template_body(template)
        .send()
        .await
        .expect("UpdateStack accepts the update whatever happens to it next");
}

async fn describe(cfn: &Client, stack: &str) -> aws_sdk_cloudformation::types::Stack {
    cfn.describe_stacks()
        .stack_name(stack)
        .send()
        .await
        .unwrap()
        .stacks()
        .first()
        .unwrap()
        .clone()
}

async fn status(cfn: &Client, stack: &str) -> String {
    describe(cfn, stack)
        .await
        .stack_status()
        .unwrap()
        .as_str()
        .to_string()
}

/// Every event in the order it happened.
async fn events(cfn: &Client, stack: &str) -> Vec<StackEvent> {
    let mut all = cfn
        .describe_stack_events()
        .stack_name(stack)
        .send()
        .await
        .unwrap()
        .stack_events()
        .to_vec();
    // DescribeStackEvents lists the newest first.
    all.reverse();
    all
}

/// The events since the last `User Initiated` update, as
/// `(logical id, status, physical id, reason)`.
async fn last_operation(cfn: &Client, stack: &str) -> Vec<(String, String, String, String)> {
    let all = events(cfn, stack).await;
    let start = all
        .iter()
        .rposition(|e| {
            e.resource_status_reason() == Some("User Initiated")
                && e.resource_type() == Some(STACK_TYPE)
        })
        .expect("an operation started");
    all[start..]
        .iter()
        .map(|e| {
            (
                e.logical_resource_id().unwrap().to_string(),
                e.resource_status().unwrap().as_str().to_string(),
                e.physical_resource_id().unwrap_or("").to_string(),
                e.resource_status_reason().unwrap_or("").to_string(),
            )
        })
        .collect()
}

type Row = (String, String, String, String);

/// The rows of one logical id, as `(status, physical id, reason)`.
fn rows_of<'a>(rows: &'a [Row], logical: &str) -> Vec<(&'a str, &'a str, &'a str)> {
    rows.iter()
        .filter(|r| r.0 == logical)
        .map(|r| (r.1.as_str(), r.2.as_str(), r.3.as_str()))
        .collect()
}

fn position(rows: &[Row], logical: &str, wanted: &str) -> usize {
    rows.iter()
        .position(|r| r.0 == logical && r.1 == wanted)
        .unwrap_or_else(|| panic!("no {logical} {wanted} in {rows:#?}"))
}

/// The physical ids by logical id.
async fn physical_ids(cfn: &Client, stack: &str) -> Vec<(String, String, String)> {
    let mut out: Vec<(String, String, String)> = cfn
        .list_stack_resources()
        .stack_name(stack)
        .send()
        .await
        .unwrap()
        .stack_resource_summaries()
        .iter()
        .map(|r| {
            (
                r.logical_resource_id().unwrap().to_string(),
                r.physical_resource_id().unwrap().to_string(),
                r.resource_status().unwrap().as_str().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

async fn template_of(cfn: &Client, stack: &str) -> Value {
    let body = cfn
        .get_template()
        .stack_name(stack)
        .send()
        .await
        .unwrap()
        .template_body()
        .unwrap()
        .to_string();
    serde_json::from_str(&body).unwrap()
}

/// E1: a replacement with an unchanged custom physical name fails after being
/// accepted, even with `UpdateReplacePolicy: Retain`, and leaves the table alone.
async fn custom_named_replacement_refused(retain: bool) {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let dynamodb = server.dynamodb_client().await;
    let stack = "cfn-e1";

    let mut before = table(Some("cfn-e1-table"), "a");
    if retain {
        before["UpdateReplacePolicy"] = json!("Retain");
    }
    let mut after = table(Some("cfn-e1-table"), "b");
    if retain {
        after["UpdateReplacePolicy"] = json!("Retain");
    }
    let original = resources(json!({"T": before}));
    let stack_id = create(&cfn, stack, &original).await;
    update(&cfn, stack, &resources(json!({"T": after}))).await;

    let rows = last_operation(&cfn, stack).await;
    let reason = "CloudFormation cannot update a stack when a custom-named resource requires replacing. Rename cfn-e1-table and update the stack again.";
    assert_eq!(
        rows,
        vec![
            row(stack, "UPDATE_IN_PROGRESS", &stack_id, "User Initiated"),
            row("T", "UPDATE_IN_PROGRESS", "cfn-e1-table", ""),
            row("T", "UPDATE_FAILED", "cfn-e1-table", reason),
            row(
                stack,
                "UPDATE_ROLLBACK_IN_PROGRESS",
                &stack_id,
                "The following resource(s) failed to update: [T]. "
            ),
            row("T", "UPDATE_COMPLETE", "cfn-e1-table", ""),
            row(
                stack,
                "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                &stack_id,
                ""
            ),
            row(stack, "UPDATE_ROLLBACK_COMPLETE", &stack_id, ""),
        ]
    );

    let described = describe(&cfn, stack).await;
    assert_eq!(
        described.stack_status().unwrap().as_str(),
        "UPDATE_ROLLBACK_COMPLETE"
    );
    assert_eq!(described.stack_status_reason(), None);
    assert_eq!(
        physical_ids(&cfn, stack).await,
        vec![(
            "T".to_string(),
            "cfn-e1-table".to_string(),
            "UPDATE_COMPLETE".to_string()
        )]
    );
    let stored: Value = serde_json::from_str(&original).unwrap();
    assert_eq!(template_of(&cfn, stack).await, stored);
    let schema = dynamodb
        .describe_table()
        .table_name("cfn-e1-table")
        .send()
        .await
        .unwrap();
    let key = schema.table().unwrap().key_schema()[0].attribute_name();
    assert_eq!(key, "a", "the table is untouched");
}

fn row(logical: &str, status: &str, physical: &str, reason: &str) -> Row {
    (
        logical.to_string(),
        status.to_string(),
        physical.to_string(),
        reason.to_string(),
    )
}

#[tokio::test]
async fn e1_custom_named_replacement_is_refused() {
    custom_named_replacement_refused(false).await;
}

#[tokio::test]
async fn e1b_custom_named_replacement_is_refused_even_with_retain() {
    custom_named_replacement_refused(true).await;
}

/// Recorded: SQS has no custom-name failure. The replacement starts, its handler
/// refuses the taken name, and a failed new resource is accounted for in cleanup.
/// Firehose is unrecorded and treated the same way.
#[tokio::test]
async fn a_handler_type_refuses_a_same_name_replacement_itself() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;

    let named = |fifo: bool| {
        let mut properties = json!({"QueueName": "cfn-e1-queue.fifo", "ContentBasedDeduplication": true, "FifoQueue": true});
        if !fifo {
            properties.as_object_mut().unwrap().remove("FifoQueue");
        }
        json!({"Q": {"Type": "AWS::SQS::Queue", "Properties": properties}})
    };
    let stack_id = create(&cfn, "cfn-e1-sqs", &resources(named(true))).await;
    let queue_url = physical_ids(&cfn, "cfn-e1-sqs").await[0].1.clone();
    update(&cfn, "cfn-e1-sqs", &resources(named(false))).await;
    let rows = last_operation(&cfn, "cfn-e1-sqs").await;
    assert_eq!(rows.len(), 8, "{rows:#?}");
    assert_eq!(
        rows[1],
        row(
            "Q",
            "UPDATE_IN_PROGRESS",
            &queue_url,
            "Requested update requires the creation of a new physical resource; hence creating one."
        )
    );
    assert_eq!(rows[2].1, "UPDATE_FAILED");
    assert!(
        rows[2].3.starts_with(
            "Resource handler returned message: \"Queue creation failed because the queue already exists\" (RequestToken: "
        ) && rows[2].3.ends_with("HandlerErrorCode: AlreadyExists)"),
        "{rows:#?}"
    );
    assert_eq!(
        rows[3],
        row(
            "cfn-e1-sqs",
            "UPDATE_ROLLBACK_IN_PROGRESS",
            &stack_id,
            "The following resource(s) failed to update: [Q]. "
        )
    );
    assert_eq!(rows[4], row("Q", "UPDATE_COMPLETE", &queue_url, ""));
    assert_eq!(rows[5].1, "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS");
    assert_eq!(rows[6], row("Q", "DELETE_COMPLETE", "", ""));
    assert_eq!(rows[7].1, "UPDATE_ROLLBACK_COMPLETE");
    sqs.get_queue_url()
        .queue_name("cfn-e1-queue.fifo")
        .send()
        .await
        .expect("the queue is untouched");

    let stream = |stream_type: Option<&str>| {
        let mut properties = json!({
            "DeliveryStreamName": "cfn-e1-stream",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/fh",
                "BucketARN": "arn:aws:s3:::bucket"
            }
        });
        if let Some(stream_type) = stream_type {
            properties["DeliveryStreamType"] = json!(stream_type);
            properties["KinesisStreamSourceConfiguration"] = json!({
                "KinesisStreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
                "RoleARN": "arn:aws:iam::123456789012:role/fh"
            });
        }
        json!({"S": {"Type": "AWS::KinesisFirehose::DeliveryStream", "Properties": properties}})
    };
    create(&cfn, "cfn-e1-fh", &resources(stream(None))).await;
    update(
        &cfn,
        "cfn-e1-fh",
        &resources(stream(Some("KinesisStreamAsSource"))),
    )
    .await;
    assert_eq!(status(&cfn, "cfn-e1-fh").await, "UPDATE_ROLLBACK_COMPLETE");
    let rows = last_operation(&cfn, "cfn-e1-fh").await;
    assert!(
        rows.iter().any(|r| r.1 == "UPDATE_FAILED"
            && r.3.contains("HandlerErrorCode: AlreadyExists")
            && r.3.contains("cfn-e1-stream")),
        "{rows:#?}"
    );
    assert!(rows
        .iter()
        .any(|r| r.0 == "S" && r.1 == "DELETE_COMPLETE" && r.2.is_empty()));
}

/// E2: a logical rename keeping the physical name in the same stack fails
/// validation: stack events only, and the old resource keeps its identity.
#[tokio::test]
async fn e2_logical_rename_keeping_the_name_fails_validation() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let dynamodb = server.dynamodb_client().await;
    let stack = "cfn-e2";

    let stack_id = create(
        &cfn,
        stack,
        &resources(json!({"A": table(Some("cfn-e2-table"), "a")})),
    )
    .await;
    update(
        &cfn,
        stack,
        &resources(json!({"B": table(Some("cfn-e2-table"), "a")})),
    )
    .await;

    let rows = last_operation(&cfn, stack).await;
    assert_eq!(
        rows,
        vec![
            row(stack, "UPDATE_IN_PROGRESS", &stack_id, "User Initiated"),
            row(
                stack,
                "UPDATE_FAILED",
                &stack_id,
                "Validation failed with 1 error(s). Call DescribeEvents to retrieve the full list of issues with resource and property details, resolve each error, then retry the operation."
            ),
            row(
                stack,
                "UPDATE_ROLLBACK_IN_PROGRESS",
                &stack_id,
                "Validation failure detected. See operation's FAILED event for details."
            ),
            row(
                stack,
                "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                &stack_id,
                ""
            ),
            row(stack, "UPDATE_ROLLBACK_COMPLETE", &stack_id, ""),
        ]
    );
    assert_eq!(
        physical_ids(&cfn, stack).await,
        vec![(
            "A".to_string(),
            "cfn-e2-table".to_string(),
            "CREATE_COMPLETE".to_string()
        )]
    );
    dynamodb
        .describe_table()
        .table_name("cfn-e2-table")
        .send()
        .await
        .expect("the table survives");

    // DescribeEvents holds the issue the stack event points at, in the recorded
    // operation shape.
    let listed = cfn
        .describe_events()
        .stack_name(stack)
        .send()
        .await
        .unwrap();
    let all = listed.operation_events();
    let of_type = |wanted: &str| {
        all.iter()
            .find(|e| e.event_type().map(|t| t.as_str()) == Some(wanted))
            .unwrap_or_else(|| panic!("no {wanted} in {all:#?}"))
    };
    let issue = of_type("VALIDATION_ERROR");
    assert_eq!(issue.logical_resource_id(), Some("B"));
    assert_eq!(issue.resource_type(), Some("AWS::DynamoDB::Table"));
    assert_eq!(
        issue.operation_type().map(|t| t.as_str()),
        Some("UPDATE_STACK")
    );
    assert_eq!(
        issue.validation_failure_mode().map(|m| m.as_str()),
        Some("FAIL")
    );
    assert_eq!(issue.validation_name(), Some("NAME_CONFLICT_VALIDATION"));
    assert_eq!(
        issue.validation_status().map(|s| s.as_str()),
        Some("FAILED")
    );
    assert_eq!(
        issue.validation_status_reason(),
        Some("Resource of type 'AWS::DynamoDB::Table' with identifier 'cfn-e2-table' already exists.")
    );
    assert_eq!(issue.validation_path(), Some("/Resources/B"));
    let error = of_type("PROVISIONING_ERROR");
    assert_eq!(
        error.detailed_status().map(|s| s.as_str()),
        Some("VALIDATION_FAILED")
    );
    assert_eq!(error.logical_resource_id(), Some(stack));
    // The update's operation failed; the rollback is an operation of its own.
    let closing = |kind: &str, status: &str| {
        all.iter().any(|e| {
            e.event_type().map(|t| t.as_str()) == Some("STACK_EVENT")
                && e.operation_type().map(|t| t.as_str()) == Some(kind)
                && e.operation_status().map(|s| s.as_str()) == Some(status)
        })
    };
    assert!(closing("UPDATE_STACK", "IN_PROGRESS"));
    assert!(closing("UPDATE_STACK", "FAILED"));
    assert!(closing("ROLLBACK", "IN_PROGRESS"));
    assert!(closing("ROLLBACK", "SUCCEEDED"));
    // Newest first.
    assert_eq!(
        all[0].operation_status().map(|s| s.as_str()),
        Some("SUCCEEDED")
    );
}

/// Recorded: the same validation fires when the name's holder stays in the
/// template, and when it is in another stack.
#[tokio::test]
async fn e2_validation_also_covers_a_kept_holder_and_another_stack() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    let kept = resources(json!({"K": table(Some("cfn-e2-held"), "a")}));
    let stack_id = create(&cfn, "cfn-e2k", &kept).await;
    update(
        &cfn,
        "cfn-e2k",
        &resources(json!({
            "K": table(Some("cfn-e2-held"), "a"),
            "Second": table(Some("cfn-e2-held"), "a"),
        })),
    )
    .await;
    let statuses = |rows: &[Row]| -> Vec<String> { rows.iter().map(|r| r.1.clone()).collect() };
    let expected = vec![
        "UPDATE_IN_PROGRESS",
        "UPDATE_FAILED",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
        "UPDATE_ROLLBACK_COMPLETE",
    ];
    let rows = last_operation(&cfn, "cfn-e2k").await;
    assert_eq!(statuses(&rows), expected);
    assert!(rows.iter().all(|r| r.2 == stack_id), "no resource events");

    let other_id = create(&cfn, "cfn-e2o", &resources(json!({"Z": table(None, "a")}))).await;
    update(
        &cfn,
        "cfn-e2o",
        &resources(json!({
            "Z": table(None, "a"),
            "Other": table(Some("cfn-e2-held"), "a"),
        })),
    )
    .await;
    let rows = last_operation(&cfn, "cfn-e2o").await;
    assert_eq!(statuses(&rows), expected);
    assert!(rows.iter().all(|r| r.2 == other_id), "no resource events");
}

fn e3_before() -> String {
    resources(json!({
        "Keep": queue(30),
        "Removed": {"Type": "AWS::SNS::Topic", "Properties": {}},
        "Replaced": table(None, "a"),
    }))
}

fn e3_after(bad: bool) -> String {
    let mut entries = json!({
        "Keep": queue(60),
        "Added": queue(45),
        "Replaced": table(None, "b"),
    });
    if bad {
        entries["Added"]["Properties"]["QueueName"] = json!("cfn-e3-added");
        entries["Bad"] = json!({
            "Type": "AWS::SQS::Queue",
            "DependsOn": ["Added", "Keep", "Replaced"],
            "Properties": {"QueueName": "not a valid name!"}
        });
    }
    resources(entries)
}

/// E3: create, update and replace happen first and the removed resource is never
/// touched. On failure the update is undone: the update re-applied, the
/// replacement pointed back, then the new ones deleted in cleanup.
#[tokio::test]
async fn e3_failed_update_really_rolls_back() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;
    let dynamodb = server.dynamodb_client().await;
    let stack = "cfn-e3";

    let original = e3_before();
    let stack_id = create(&cfn, stack, &original).await;
    let before = physical_ids(&cfn, stack).await;
    let id_of = |ids: &[(String, String, String)], logical: &str| {
        ids.iter()
            .find(|r| r.0 == logical)
            .unwrap_or_else(|| panic!("no {logical}"))
            .1
            .clone()
    };
    let keep = id_of(&before, "Keep");
    let removed = id_of(&before, "Removed");
    let old_table = id_of(&before, "Replaced");

    update(&cfn, stack, &e3_after(true)).await;

    let rows = last_operation(&cfn, stack).await;
    let added_url = rows
        .iter()
        .find(|r| r.0 == "Added" && !r.2.is_empty())
        .unwrap()
        .2
        .clone();
    assert!(added_url.ends_with("/cfn-e3-added"), "{added_url}");
    let new_table = rows
        .iter()
        .find(|r| r.0 == "Replaced" && r.3 == "Resource creation Initiated")
        .unwrap()
        .2
        .clone();
    assert_ne!(new_table, old_table, "a new physical resource, new name");

    assert_eq!(
        rows_of(&rows, "Added"),
        vec![
            ("CREATE_IN_PROGRESS", "", ""),
            (
                "CREATE_IN_PROGRESS",
                added_url.as_str(),
                "Resource creation Initiated"
            ),
            ("CREATE_COMPLETE", added_url.as_str(), ""),
            ("DELETE_IN_PROGRESS", added_url.as_str(), ""),
            ("DELETE_COMPLETE", added_url.as_str(), ""),
        ]
    );
    assert_eq!(
        rows_of(&rows, "Keep"),
        vec![
            ("UPDATE_IN_PROGRESS", keep.as_str(), ""),
            ("UPDATE_COMPLETE", keep.as_str(), ""),
            ("UPDATE_IN_PROGRESS", keep.as_str(), ""),
            ("UPDATE_COMPLETE", keep.as_str(), ""),
        ]
    );
    assert_eq!(
        rows_of(&rows, "Replaced"),
        vec![
            ("UPDATE_IN_PROGRESS", old_table.as_str(), ""),
            (
                "UPDATE_IN_PROGRESS",
                old_table.as_str(),
                "Requested update requires the creation of a new physical resource; hence creating one."
            ),
            (
                "UPDATE_IN_PROGRESS",
                new_table.as_str(),
                "Resource creation Initiated"
            ),
            ("UPDATE_COMPLETE", new_table.as_str(), ""),
            // Pointed back, with no in-progress event.
            ("UPDATE_COMPLETE", old_table.as_str(), ""),
            ("DELETE_IN_PROGRESS", new_table.as_str(), ""),
            ("DELETE_COMPLETE", new_table.as_str(), ""),
        ]
    );
    let bad = rows_of(&rows, "Bad");
    assert_eq!(bad.len(), 3, "{bad:?}");
    assert_eq!(bad[0], ("CREATE_IN_PROGRESS", "", ""));
    assert_eq!(bad[1].0, "CREATE_FAILED");
    assert_eq!(bad[1].1, "");
    assert!(
        bad[1]
            .2
            .contains("Can only include alphanumeric characters"),
        "{bad:?}"
    );
    assert_eq!(bad[2], ("DELETE_COMPLETE", "", ""));
    assert!(
        rows_of(&rows, "Removed").is_empty(),
        "the removed resource is never touched: {rows:#?}"
    );

    // The phases, in order.
    let rollback = position(&rows, stack, "UPDATE_ROLLBACK_IN_PROGRESS");
    assert_eq!(
        rows[rollback].3,
        "The following resource(s) failed to create: [Bad]. "
    );
    let cleanup = position(&rows, stack, "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS");
    let complete = position(&rows, stack, "UPDATE_ROLLBACK_COMPLETE");
    assert!(position(&rows, "Bad", "CREATE_FAILED") < rollback);
    assert!(rollback < cleanup && cleanup < complete);
    assert!(cleanup < position(&rows, "Added", "DELETE_IN_PROGRESS"));
    assert!(cleanup < position(&rows, "Bad", "DELETE_COMPLETE"));
    assert!(position(&rows, "Added", "DELETE_COMPLETE") < complete);

    // Final state.
    assert_eq!(
        physical_ids(&cfn, stack).await,
        vec![
            (
                "Keep".to_string(),
                keep.clone(),
                "UPDATE_COMPLETE".to_string()
            ),
            (
                "Removed".to_string(),
                removed.clone(),
                "CREATE_COMPLETE".to_string()
            ),
            (
                "Replaced".to_string(),
                old_table.clone(),
                "UPDATE_COMPLETE".to_string()
            ),
        ]
    );
    let stored: Value = serde_json::from_str(&original).unwrap();
    assert_eq!(template_of(&cfn, stack).await, stored);
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&keep)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attrs
            .attributes()
            .unwrap()
            .get(&aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
            .map(String::as_str),
        Some("30"),
        "the update is undone in the service"
    );
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-e3-added")
        .send()
        .await
        .is_err());
    sns.get_topic_attributes()
        .topic_arn(&removed)
        .send()
        .await
        .expect("the removed topic survives");
    dynamodb
        .describe_table()
        .table_name(&old_table)
        .send()
        .await
        .expect("the old table survives");
    assert!(dynamodb
        .describe_table()
        .table_name(&new_table)
        .send()
        .await
        .is_err());

    // E5 on the same stack: the same update without the bad resource applies,
    // and only then are the removed and the replaced resources deleted.
    update(&cfn, stack, &e3_after(false)).await;
    let rows = last_operation(&cfn, stack).await;
    let added_url = rows
        .iter()
        .find(|r| r.0 == "Added" && !r.2.is_empty())
        .unwrap()
        .2
        .clone();
    let new_table = rows
        .iter()
        .find(|r| r.0 == "Replaced" && r.3 == "Resource creation Initiated")
        .unwrap()
        .2
        .clone();
    assert_ne!(new_table, old_table);
    assert_eq!(
        rows_of(&rows, "Added"),
        vec![
            ("CREATE_IN_PROGRESS", "", ""),
            (
                "CREATE_IN_PROGRESS",
                added_url.as_str(),
                "Resource creation Initiated"
            ),
            ("CREATE_COMPLETE", added_url.as_str(), ""),
        ]
    );
    assert_eq!(
        rows_of(&rows, "Keep"),
        vec![
            ("UPDATE_IN_PROGRESS", keep.as_str(), ""),
            ("UPDATE_COMPLETE", keep.as_str(), ""),
        ]
    );
    assert_eq!(
        rows_of(&rows, "Replaced"),
        vec![
            ("UPDATE_IN_PROGRESS", old_table.as_str(), ""),
            (
                "UPDATE_IN_PROGRESS",
                old_table.as_str(),
                "Requested update requires the creation of a new physical resource; hence creating one."
            ),
            (
                "UPDATE_IN_PROGRESS",
                new_table.as_str(),
                "Resource creation Initiated"
            ),
            ("UPDATE_COMPLETE", new_table.as_str(), ""),
            ("DELETE_IN_PROGRESS", old_table.as_str(), ""),
            ("DELETE_COMPLETE", old_table.as_str(), ""),
        ]
    );
    assert_eq!(
        rows_of(&rows, "Removed"),
        vec![
            ("DELETE_IN_PROGRESS", removed.as_str(), ""),
            ("DELETE_COMPLETE", removed.as_str(), ""),
        ]
    );
    let cleanup = position(&rows, stack, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS");
    let complete = position(&rows, stack, "UPDATE_COMPLETE");
    assert_eq!(complete, rows.len() - 1);
    assert!(position(&rows, "Replaced", "UPDATE_COMPLETE") < cleanup);
    assert!(position(&rows, "Added", "CREATE_COMPLETE") < cleanup);
    assert!(cleanup < position(&rows, "Removed", "DELETE_IN_PROGRESS"));
    assert!(cleanup < position(&rows, "Replaced", "DELETE_IN_PROGRESS"));
    assert_eq!(rows[0].2, stack_id);

    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    assert_eq!(
        physical_ids(&cfn, stack).await,
        vec![
            (
                "Added".to_string(),
                added_url.clone(),
                "CREATE_COMPLETE".to_string()
            ),
            (
                "Keep".to_string(),
                keep.clone(),
                "UPDATE_COMPLETE".to_string()
            ),
            (
                "Replaced".to_string(),
                new_table.clone(),
                "UPDATE_COMPLETE".to_string()
            ),
        ]
    );
    assert!(sns
        .get_topic_attributes()
        .topic_arn(&removed)
        .send()
        .await
        .is_err());
    assert!(dynamodb
        .describe_table()
        .table_name(&old_table)
        .send()
        .await
        .is_err());
    dynamodb
        .describe_table()
        .table_name(&new_table)
        .send()
        .await
        .expect("the new table is the stack's");
}

/// E4: a literal export name another stack owns: accepted, then rolled back with
/// no resource touched, and the new resource never created.
#[tokio::test]
async fn e4_export_collision_rolls_back_without_touching_a_resource() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;

    let exporting = json!({
        "Resources": {"Q": queue(30)},
        "Outputs": {"O": {"Value": "v", "Export": {"Name": "cfn-e4-export"}}}
    })
    .to_string();
    create(&cfn, "cfn-e4a", &exporting).await;

    let plain = resources(json!({"Q": queue(30)}));
    let stack_id = create(&cfn, "cfn-e4b", &plain).await;
    let before = physical_ids(&cfn, "cfn-e4b").await;

    let colliding = json!({
        "Resources": {
            "Q": queue(60),
            "New": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-e4-new"}}
        },
        "Outputs": {"O": {"Value": "v", "Export": {"Name": "cfn-e4-export"}}}
    })
    .to_string();
    update(&cfn, "cfn-e4b", &colliding).await;

    assert_eq!(
        last_operation(&cfn, "cfn-e4b").await,
        vec![
            row("cfn-e4b", "UPDATE_IN_PROGRESS", &stack_id, "User Initiated"),
            row(
                "cfn-e4b",
                "UPDATE_ROLLBACK_IN_PROGRESS",
                &stack_id,
                "Export with name cfn-e4-export is already exported by stack cfn-e4a"
            ),
            row(
                "cfn-e4b",
                "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                &stack_id,
                ""
            ),
            row("cfn-e4b", "UPDATE_ROLLBACK_COMPLETE", &stack_id, ""),
        ]
    );
    assert_eq!(physical_ids(&cfn, "cfn-e4b").await, before);
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-e4-new")
        .send()
        .await
        .is_err());
    let exports = cfn.list_exports().send().await.unwrap();
    let export = exports
        .exports()
        .iter()
        .find(|e| e.name() == Some("cfn-e4-export"))
        .expect("still exported");
    assert_eq!(
        export.exporting_stack_id().map(|id| id.contains("cfn-e4a")),
        Some(true)
    );
}

/// E4b: an export name built from a resource is refused at the API, by every
/// operation that takes a template.
#[tokio::test]
async fn e4b_export_name_depending_on_a_resource_is_refused_at_the_api() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let expected = "Template format error: Output O is malformed.  The Name field of Export must not depend on any resources, imported values, or Fn::GetAZs.";

    let plain = resources(json!({"Q": queue(30)}));
    create(&cfn, "cfn-e4c", &plain).await;

    for name in [
        json!({"Fn::GetAtt": ["Q", "QueueName"]}),
        json!({"Ref": "Q"}),
        json!({"Fn::Sub": "${Q}-export"}),
        json!({"Fn::ImportValue": "elsewhere"}),
        json!({"Fn::Join": ["-", [{"Fn::GetAZs": ""}, "x"]]}),
    ] {
        let body = json!({
            "Resources": {"Q": queue(30)},
            "Outputs": {"O": {"Value": "v", "Export": {"Name": name}}}
        })
        .to_string();

        let err = cfn
            .update_stack()
            .stack_name("cfn-e4c")
            .template_body(&body)
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(err.meta().code(), Some("ValidationError"));
        assert_eq!(err.meta().message(), Some(expected));

        let err = cfn
            .create_stack()
            .stack_name("cfn-e4d")
            .template_body(&body)
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(err.meta().message(), Some(expected));

        let err = cfn
            .create_change_set()
            .stack_name("cfn-e4c")
            .change_set_name("cs")
            .template_body(&body)
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(err.meta().message(), Some(expected));
    }
    assert_eq!(status(&cfn, "cfn-e4c").await, "CREATE_COMPLETE");

    // Parameters and pseudo parameters are fine.
    let body = json!({
        "Parameters": {"Env": {"Type": "String", "Default": "dev"}},
        "Resources": {"Q": queue(30)},
        "Outputs": {"O": {
            "Value": {"Ref": "Q"},
            "Export": {"Name": {"Fn::Sub": "${AWS::StackName}-${Env}-q"}}
        }}
    })
    .to_string();
    update(&cfn, "cfn-e4c", &body).await;
    assert_eq!(status(&cfn, "cfn-e4c").await, "UPDATE_COMPLETE");
    let exports = cfn.list_exports().send().await.unwrap();
    assert!(exports
        .exports()
        .iter()
        .any(|e| e.name() == Some("cfn-e4c-dev-q")));
}

/// E6: a replacement renaming into a name another resource of the stack holds.
#[tokio::test]
async fn e6_rename_into_an_occupied_name_fails_and_rolls_back() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let dynamodb = server.dynamodb_client().await;
    let stack = "cfn-e6";

    let stack_id = create(
        &cfn,
        stack,
        &resources(json!({
            "Other": table(Some("cfn-e6-other"), "a"),
            "Mine": table(Some("cfn-e6-mine"), "a"),
        })),
    )
    .await;
    update(
        &cfn,
        stack,
        &resources(json!({
            "Other": table(Some("cfn-e6-other"), "a"),
            "Mine": table(Some("cfn-e6-other"), "a"),
        })),
    )
    .await;

    assert_eq!(
        last_operation(&cfn, stack).await,
        vec![
            row(stack, "UPDATE_IN_PROGRESS", &stack_id, "User Initiated"),
            row(
                "Mine",
                "UPDATE_IN_PROGRESS",
                "cfn-e6-mine",
                "Requested update requires the creation of a new physical resource; hence creating one."
            ),
            row(
                "Mine",
                "UPDATE_FAILED",
                "cfn-e6-mine",
                &format!("cfn-e6-other already exists in stack {stack_id}")
            ),
            row(
                stack,
                "UPDATE_ROLLBACK_IN_PROGRESS",
                &stack_id,
                "The following resource(s) failed to update: [Mine]. "
            ),
            row("Mine", "UPDATE_COMPLETE", "cfn-e6-mine", ""),
            row(
                stack,
                "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                &stack_id,
                ""
            ),
            row("Mine", "DELETE_COMPLETE", "", ""),
            row(stack, "UPDATE_ROLLBACK_COMPLETE", &stack_id, ""),
        ]
    );
    assert_eq!(
        physical_ids(&cfn, stack).await,
        vec![
            (
                "Mine".to_string(),
                "cfn-e6-mine".to_string(),
                "UPDATE_COMPLETE".to_string()
            ),
            (
                "Other".to_string(),
                "cfn-e6-other".to_string(),
                "CREATE_COMPLETE".to_string()
            ),
        ]
    );
    for name in ["cfn-e6-mine", "cfn-e6-other"] {
        dynamodb
            .describe_table()
            .table_name(name)
            .send()
            .await
            .expect("both tables survive");
    }
}

/// Recorded: a replacement is refused only when it would produce the same
/// physical identity. A named rule moved to another bus keeps its name but gets a
/// new physical id, so it is replaced, and cleanup deletes the old one.
#[tokio::test]
async fn named_rule_moved_to_another_bus_is_replaced_not_refused() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let events_client = server.eventbridge_client().await;
    let stack = "cfn-rule";

    let bus = json!({"Type": "AWS::Events::EventBus", "Properties": {"Name": "cfn-rule-bus"}});
    let rule = |bus: Option<&str>| {
        let mut properties = json!({
            "Name": "cfn-rule",
            "EventPattern": {"source": ["cfn.test"]}
        });
        if let Some(bus) = bus {
            properties["EventBusName"] = json!(bus);
        }
        json!({"Type": "AWS::Events::Rule", "DependsOn": "Bus", "Properties": properties})
    };
    create(
        &cfn,
        stack,
        &resources(json!({"Bus": bus, "R": rule(None)})),
    )
    .await;
    let old = physical_ids(&cfn, stack)
        .await
        .into_iter()
        .find(|r| r.0 == "R")
        .unwrap()
        .1;
    update(
        &cfn,
        stack,
        &resources(json!({"Bus": bus, "R": rule(Some("cfn-rule-bus"))})),
    )
    .await;

    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    let new = physical_ids(&cfn, stack)
        .await
        .into_iter()
        .find(|r| r.0 == "R")
        .unwrap()
        .1;
    assert_ne!(new, old);
    let rows = last_operation(&cfn, stack).await;
    assert_eq!(
        rows_of(&rows, "R"),
        vec![
            ("UPDATE_IN_PROGRESS", old.as_str(), ""),
            (
                "UPDATE_IN_PROGRESS",
                old.as_str(),
                "Requested update requires the creation of a new physical resource; hence creating one."
            ),
            (
                "UPDATE_IN_PROGRESS",
                new.as_str(),
                "Resource creation Initiated"
            ),
            ("UPDATE_COMPLETE", new.as_str(), ""),
            ("DELETE_IN_PROGRESS", old.as_str(), ""),
            ("DELETE_COMPLETE", old.as_str(), ""),
        ]
    );
    events_client
        .describe_rule()
        .name("cfn-rule")
        .event_bus_name("cfn-rule-bus")
        .send()
        .await
        .expect("the new rule is on the new bus");
    assert!(
        events_client
            .describe_rule()
            .name("cfn-rule")
            .send()
            .await
            .is_err(),
        "the old rule is gone from the default bus"
    );
}

async fn execute(cfn: &Client, stack: &str, name: &str, template: &str, kind: ChangeSetType) {
    cfn.create_change_set()
        .stack_name(stack)
        .change_set_name(name)
        .change_set_type(kind)
        .template_body(template)
        .send()
        .await
        .expect("create_change_set");
    cfn.execute_change_set()
        .stack_name(stack)
        .change_set_name(name)
        .send()
        .await
        .expect("ExecuteChangeSet accepts the execution whatever happens to it next");
}

/// The change-set door goes through the same engine.
#[tokio::test]
async fn change_set_execution_rolls_back_like_update_stack() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;
    let stack = "cfn-cs";

    execute(&cfn, stack, "create", &e3_before(), ChangeSetType::Create).await;
    assert_eq!(status(&cfn, stack).await, "CREATE_COMPLETE");
    let before = physical_ids(&cfn, stack).await;
    let keep = before.iter().find(|r| r.0 == "Keep").unwrap().1.clone();

    execute(&cfn, stack, "bad", &e3_after(true), ChangeSetType::Update).await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    let rows = last_operation(&cfn, stack).await;
    assert!(rows_of(&rows, "Removed").is_empty(), "{rows:#?}");
    assert!(position(&rows, "Added", "DELETE_COMPLETE") > 0);
    let description = cfn
        .describe_change_set()
        .stack_name(stack)
        .change_set_name("bad")
        .send()
        .await
        .unwrap();
    assert_eq!(
        description.execution_status().unwrap().as_str(),
        "EXECUTE_FAILED"
    );
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&keep)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attrs
            .attributes()
            .unwrap()
            .get(&aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
            .map(String::as_str),
        Some("30")
    );
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-e3-added")
        .send()
        .await
        .is_err());

    execute(&cfn, stack, "good", &e3_after(false), ChangeSetType::Update).await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    let rows = last_operation(&cfn, stack).await;
    assert!(position(&rows, stack, "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS") > 0);
    assert!(position(&rows, "Removed", "DELETE_COMPLETE") > 0);

    // A CREATE change set that fails deletes what it made.
    let doomed = resources(json!({
        "Fine": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-cs-fine"}},
        "Bad": {
            "Type": "AWS::SQS::Queue",
            "DependsOn": "Fine",
            "Properties": {"QueueName": "not a valid name!"}
        }
    }));
    execute(&cfn, "cfn-cs2", "create", &doomed, ChangeSetType::Create).await;
    assert_eq!(status(&cfn, "cfn-cs2").await, "ROLLBACK_COMPLETE");
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-cs-fine")
        .send()
        .await
        .is_err());
    assert!(physical_ids(&cfn, "cfn-cs2").await.is_empty());
}

/// Change sets predict the same classification the execution applies.
#[tokio::test]
async fn change_set_reports_replacement_from_the_schema_facts() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let stack = "cfn-cs-plan";

    create(
        &cfn,
        stack,
        &resources(json!({
            "Same": queue(30),
            "Tuned": queue(30),
            "Renamed": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-plan-a"}},
            "Gone": queue(30),
        })),
    )
    .await;
    cfn.create_change_set()
        .stack_name(stack)
        .change_set_name("plan")
        .template_body(resources(json!({
            "Same": queue(30),
            "Tuned": queue(60),
            "Renamed": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-plan-b"}},
            "New": queue(30),
        })))
        .send()
        .await
        .unwrap();
    let plan = cfn
        .describe_change_set()
        .stack_name(stack)
        .change_set_name("plan")
        .send()
        .await
        .unwrap();
    let mut rows: Vec<(String, String, String)> = plan
        .changes()
        .iter()
        .map(|c| {
            let change = c.resource_change().unwrap();
            (
                change.logical_resource_id().unwrap().to_string(),
                change.action().unwrap().as_str().to_string(),
                change
                    .replacement()
                    .map(|r| r.as_str().to_string())
                    .unwrap_or_default(),
            )
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("Gone".to_string(), "Remove".to_string(), String::new()),
            ("New".to_string(), "Add".to_string(), String::new()),
            (
                "Renamed".to_string(),
                "Modify".to_string(),
                "True".to_string()
            ),
            (
                "Tuned".to_string(),
                "Modify".to_string(),
                "False".to_string()
            ),
        ]
    );
}

/// `DisableRollback` is inferred from the API model and docs rather than
/// recorded: the update stops at `UPDATE_FAILED` keeping what it applied, and
/// `RollbackStack` finishes the job later.
#[tokio::test]
async fn disable_rollback_holds_the_stack_until_rollback_stack() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;
    let stack = "cfn-hold";

    create(&cfn, stack, &e3_before()).await;
    let before = physical_ids(&cfn, stack).await;
    let keep = before.iter().find(|r| r.0 == "Keep").unwrap().1.clone();
    let old_table = before.iter().find(|r| r.0 == "Replaced").unwrap().1.clone();

    cfn.update_stack()
        .stack_name(stack)
        .template_body(e3_after(true))
        .disable_rollback(true)
        .send()
        .await
        .unwrap();
    assert_eq!(status(&cfn, stack).await, "UPDATE_FAILED");
    let visibility =
        |attrs: aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesOutput| {
            attrs
                .attributes()
                .unwrap()
                .get(&aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
                .cloned()
        };
    let held = sqs
        .get_queue_attributes()
        .queue_url(&keep)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .unwrap();
    assert_eq!(visibility(held).as_deref(), Some("60"), "the update stays");
    sqs.get_queue_url()
        .queue_name("cfn-e3-added")
        .send()
        .await
        .expect("what the update created stays");

    cfn.rollback_stack().stack_name(stack).send().await.unwrap();
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    let undone = sqs
        .get_queue_attributes()
        .queue_url(&keep)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .unwrap();
    assert_eq!(visibility(undone).as_deref(), Some("30"));
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-e3-added")
        .send()
        .await
        .is_err());
    let after = physical_ids(&cfn, stack).await;
    assert_eq!(
        after.iter().find(|r| r.0 == "Replaced").unwrap().1,
        old_table
    );

    // A stack with nothing to roll back refuses.
    let err = cfn
        .rollback_stack()
        .stack_name(stack)
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.meta().code(), Some("ValidationError"));
}

/// A nested stack added or changed by an update used to hang: it provisioned
/// under the state lock it then took itself. A nested update takes part in the
/// parent's rollback.
#[tokio::test]
async fn nested_stacks_update_and_roll_back_with_their_parent() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;
    let stack = "cfn-nested";

    let child = |visibility: u32, extra: bool| {
        let mut entries = json!({"Inner": queue(visibility)});
        if extra {
            entries["Added"] =
                json!({"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-nested-added"}});
        }
        json!({
            "Resources": entries,
            "Outputs": {"Url": {"Value": {"Ref": "Inner"}}}
        })
        .to_string()
    };
    let nested = |visibility: u32, extra: bool| {
        json!({
            "Type": "AWS::CloudFormation::Stack",
            "Properties": {"TemplateBody": child(visibility, extra)}
        })
    };

    create(&cfn, stack, &resources(json!({"Anchor": queue(30)}))).await;
    update(
        &cfn,
        stack,
        &resources(json!({"Anchor": queue(30), "Child": nested(30, false)})),
    )
    .await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    let child_id = physical_ids(&cfn, stack)
        .await
        .into_iter()
        .find(|r| r.0 == "Child")
        .unwrap()
        .1;
    let inner_url = physical_ids(&cfn, &child_id)
        .await
        .into_iter()
        .find(|r| r.0 == "Inner")
        .unwrap()
        .1;
    let child_output = describe(&cfn, &child_id)
        .await
        .outputs()
        .first()
        .unwrap()
        .output_value()
        .unwrap()
        .to_string();
    assert_eq!(child_output, inner_url);

    // The child updates, then the parent fails: the child is rolled back.
    let visibility = || async {
        sqs.get_queue_attributes()
            .queue_url(&inner_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
            .send()
            .await
            .unwrap()
            .attributes()
            .unwrap()
            .get(&aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout)
            .cloned()
            .unwrap()
    };
    update(
        &cfn,
        stack,
        &resources(json!({
            "Anchor": queue(30),
            "Child": nested(60, true),
            "Bad": {
                "Type": "AWS::SQS::Queue",
                "DependsOn": "Child",
                "Properties": {"QueueName": "not a valid name!"}
            }
        })),
    )
    .await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    assert_eq!(status(&cfn, &child_id).await, "UPDATE_ROLLBACK_COMPLETE");
    assert_eq!(visibility().await, "30");
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-nested-added")
        .send()
        .await
        .is_err());

    // A child that cannot apply fails its parent, which reports why.
    update(
        &cfn,
        stack,
        &resources(json!({
            "Anchor": queue(30),
            "Child": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {"TemplateBody": resources(json!({
                    "Inner": queue(60),
                    "Bad": {
                        "Type": "AWS::SQS::Queue",
                        "DependsOn": "Inner",
                        "Properties": {"QueueName": "not a valid name!"}
                    }
                }))}
            }
        })),
    )
    .await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    let rows = last_operation(&cfn, stack).await;
    assert!(
        rows.iter().any(|r| r.0 == "Child"
            && r.1 == "UPDATE_FAILED"
            && r.3.starts_with("Embedded stack ")),
        "{rows:#?}"
    );
    assert_eq!(visibility().await, "30");

    // The update that succeeds leaves the child updated and cleaned up.
    update(
        &cfn,
        stack,
        &resources(json!({"Anchor": queue(30), "Child": nested(45, true)})),
    )
    .await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    assert_eq!(status(&cfn, &child_id).await, "UPDATE_COMPLETE");
    assert_eq!(visibility().await, "45");
    sqs.get_queue_url()
        .queue_name("cfn-nested-added")
        .send()
        .await
        .expect("the child's new queue");
}

/// What an update did to the owning services is on disk whichever way it ended,
/// and a restart sees the same stack.
#[tokio::test]
async fn both_outcomes_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cfn = server.cloudformation_client().await;
    let stack = "cfn-persist";

    create(&cfn, stack, &e3_before()).await;
    let before = physical_ids(&cfn, stack).await;
    let old_table = before.iter().find(|r| r.0 == "Replaced").unwrap().1.clone();
    let removed = before.iter().find(|r| r.0 == "Removed").unwrap().1.clone();

    // Rolled back.
    update(&cfn, stack, &e3_after(true)).await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    drop(cfn);
    server.restart().await;
    let cfn = server.cloudformation_client().await;
    let dynamodb = server.dynamodb_client().await;
    let sqs = server.sqs_client().await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    assert_eq!(
        physical_ids(&cfn, stack)
            .await
            .iter()
            .find(|r| r.0 == "Replaced")
            .unwrap()
            .1,
        old_table
    );
    dynamodb
        .describe_table()
        .table_name(&old_table)
        .send()
        .await
        .expect("the old table is still there");
    let tables = dynamodb.list_tables().send().await.unwrap();
    assert_eq!(
        tables.table_names(),
        std::slice::from_ref(&old_table),
        "no rolled-back table"
    );
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-e3-added")
        .send()
        .await
        .is_err());

    // Applied.
    update(&cfn, stack, &e3_after(false)).await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    let after = physical_ids(&cfn, stack).await;
    let new_table = after.iter().find(|r| r.0 == "Replaced").unwrap().1.clone();
    assert_ne!(new_table, old_table);
    drop(cfn);
    server.restart().await;
    let cfn = server.cloudformation_client().await;
    let dynamodb = server.dynamodb_client().await;
    let sns = server.sns_client().await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    let tables = dynamodb.list_tables().send().await.unwrap();
    assert_eq!(
        tables.table_names(),
        [new_table],
        "the old table stays deleted"
    );
    assert!(sns
        .get_topic_attributes()
        .topic_arn(&removed)
        .send()
        .await
        .is_err());
}

/// The code and message of a refused call, whichever operation it was.
macro_rules! api_message {
    ($result:expr $(,)?) => {{
        let err = $result.expect_err("refused").into_service_error();
        (
            err.meta().code().unwrap_or_default().to_string(),
            err.meta().message().unwrap_or_default().to_string(),
        )
    }};
}

/// Recorded: a replaced resource's fate follows the new template's policy, and a
/// removed one's its stored policy.
#[tokio::test]
async fn cleanup_policies_come_from_the_recorded_templates() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let dynamodb = server.dynamodb_client().await;
    let sqs = server.sqs_client().await;
    let stack = "cfn-pol";

    let with = |key: &str, policy: Option<&str>| {
        let mut def = table(None, key);
        if let Some(policy) = policy {
            def["UpdateReplacePolicy"] = json!(policy);
        }
        def
    };
    let mut retained = queue(30);
    retained["DeletionPolicy"] = json!("Retain");
    create(
        &cfn,
        stack,
        &resources(json!({
            "T": with("a", Some("Retain")),
            "N": with("a", None),
            "R": retained,
        })),
    )
    .await;
    let before = physical_ids(&cfn, stack).await;
    let id = |logical: &str| before.iter().find(|r| r.0 == logical).unwrap().1.clone();
    let (old_t, old_n, removed_queue) = (id("T"), id("N"), id("R"));

    update(
        &cfn,
        stack,
        &resources(json!({"T": with("b", None), "N": with("b", Some("Retain"))})),
    )
    .await;
    assert_eq!(status(&cfn, stack).await, "UPDATE_COMPLETE");
    let rows = last_operation(&cfn, stack).await;
    assert!(rows
        .iter()
        .any(|r| r.0 == "T" && r.1 == "DELETE_COMPLETE" && r.2 == old_t));
    assert!(rows
        .iter()
        .any(|r| r.0 == "N" && r.1 == "DELETE_SKIPPED" && r.2 == old_n));
    assert!(rows
        .iter()
        .any(|r| r.0 == "R" && r.1 == "DELETE_SKIPPED" && r.2 == removed_queue));
    assert!(dynamodb
        .describe_table()
        .table_name(&old_t)
        .send()
        .await
        .is_err());
    dynamodb
        .describe_table()
        .table_name(&old_n)
        .send()
        .await
        .expect("kept by the new template's policy");
    sqs.get_queue_attributes()
        .queue_url(&removed_queue)
        .send()
        .await
        .expect("kept by its own stored policy");

    // Rollback: a created Retain resource stays; a new replacement does not,
    // whatever its UpdateReplacePolicy says.
    let failing = "cfn-pol-fail";
    create(&cfn, failing, &resources(json!({"T": with("a", None)}))).await;
    let old = physical_ids(&cfn, failing).await[0].1.clone();
    let mut added =
        json!({"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-pol-added"}});
    added["DeletionPolicy"] = json!("Retain");
    update(
        &cfn,
        failing,
        &resources(json!({
            "T": with("b", Some("Retain")),
            "A": added,
            "Bad": {
                "Type": "AWS::SQS::Queue",
                "DependsOn": ["A", "T"],
                "Properties": {"QueueName": "not a valid name!"}
            }
        })),
    )
    .await;
    assert_eq!(status(&cfn, failing).await, "UPDATE_ROLLBACK_COMPLETE");
    let rows = last_operation(&cfn, failing).await;
    assert!(
        rows.iter().any(|r| r.0 == "A" && r.1 == "DELETE_SKIPPED"),
        "{rows:#?}"
    );
    assert!(
        rows.iter()
            .any(|r| r.0 == "T" && r.1 == "DELETE_COMPLETE" && r.2 != old),
        "{rows:#?}"
    );
    sqs.get_queue_url()
        .queue_name("cfn-pol-added")
        .send()
        .await
        .expect("the created resource survives");
}

/// Recorded: the shape of a stack held at UPDATE_FAILED, and what each rollback
/// call says.
#[tokio::test]
async fn disable_rollback_recorded_state_and_rollback_refusals() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let stack = "cfn-dr";

    let original = resources(json!({
        "K": queue(30),
        "G": {"Type": "AWS::SNS::Topic", "Properties": {}},
    }));
    create(&cfn, stack, &original).await;

    let (code, message) = api_message!(cfn.rollback_stack().stack_name(stack).send().await);
    assert_eq!(code, "ValidationError");
    assert_eq!(
        message,
        "RollbackStack can only be called on any of the [UPDATE_FAILED, CREATE_FAILED] statuses."
    );
    let (code, message) = api_message!(
        cfn.continue_update_rollback()
            .stack_name(stack)
            .send()
            .await,
    );
    assert_eq!(code, "ValidationError");
    assert_eq!(
        message,
        "RollbackUpdatedStack cannot be called from current stack status"
    );

    let attempted = resources(json!({
        "K": queue(60),
        "A": queue(45),
        "Bad": {
            "Type": "AWS::SQS::Queue",
            "DependsOn": ["A", "K"],
            "Properties": {"QueueName": "not a valid name!"}
        }
    }));
    cfn.update_stack()
        .stack_name(stack)
        .template_body(&attempted)
        .disable_rollback(true)
        .send()
        .await
        .unwrap();
    let held = describe(&cfn, stack).await;
    let reason = "The following resource(s) failed to create: [Bad]. ";
    assert_eq!(held.stack_status().unwrap().as_str(), "UPDATE_FAILED");
    assert_eq!(held.stack_status_reason(), Some(reason));
    let rows = last_operation(&cfn, stack).await;
    assert_eq!(rows.last().unwrap().1, "UPDATE_FAILED");
    assert_eq!(rows.last().unwrap().3, reason);
    let listed = cfn
        .list_stack_resources()
        .stack_name(stack)
        .send()
        .await
        .unwrap();
    let mut states: Vec<(String, String, bool)> = listed
        .stack_resource_summaries()
        .iter()
        .map(|r| {
            (
                r.logical_resource_id().unwrap().to_string(),
                r.resource_status().unwrap().as_str().to_string(),
                r.physical_resource_id().is_some(),
            )
        })
        .collect();
    states.sort();
    assert_eq!(
        states,
        vec![
            ("A".to_string(), "CREATE_COMPLETE".to_string(), true),
            ("Bad".to_string(), "CREATE_FAILED".to_string(), false),
            ("G".to_string(), "CREATE_COMPLETE".to_string(), true),
            ("K".to_string(), "UPDATE_COMPLETE".to_string(), true),
        ]
    );
    let stored: Value = serde_json::from_str(&attempted).unwrap();
    assert_eq!(
        template_of(&cfn, stack).await,
        stored,
        "the attempted template"
    );

    let (code, message) = api_message!(
        cfn.continue_update_rollback()
            .stack_name(stack)
            .send()
            .await,
    );
    assert_eq!(code, "ValidationError");
    assert_eq!(
        message,
        "ContinueUpdateRollback cannot be called from stack with UPDATE_FAILED status."
    );

    cfn.rollback_stack().stack_name(stack).send().await.unwrap();
    assert_eq!(status(&cfn, stack).await, "UPDATE_ROLLBACK_COMPLETE");
    let all = events(&cfn, stack).await;
    assert!(all.iter().any(|e| {
        e.resource_status().map(|s| s.as_str()) == Some("UPDATE_ROLLBACK_IN_PROGRESS")
            && e.resource_status_reason() == Some("User Initiated Rollback")
    }));
    let stored: Value = serde_json::from_str(&original).unwrap();
    assert_eq!(template_of(&cfn, stack).await, stored);
    let ids: Vec<String> = physical_ids(&cfn, stack)
        .await
        .into_iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(ids, ["G", "K"]);
}

/// Recorded API-level refusals: no event follows any of them.
#[tokio::test]
async fn updates_refused_at_the_api() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let stack = "cfn-api";

    let body = resources(json!({"X": queue(30)}));
    create(&cfn, stack, &body).await;
    let count = events(&cfn, stack).await.len();

    let (code, message) = api_message!(
        cfn.update_stack()
            .stack_name(stack)
            .template_body(&body)
            .send()
            .await,
    );
    assert_eq!(code, "ValidationError");
    assert_eq!(message, "No updates are to be performed.");

    let (code, message) = api_message!(
        cfn.update_stack()
            .stack_name(stack)
            .template_body(resources(json!({"X": {"Type": "AWS::SNS::Topic"}})))
            .send()
            .await,
    );
    assert_eq!(code, "ValidationError");
    assert_eq!(
        message,
        "Update of resource type is not permitted. The new template modifies resource type of the following resources: [X]"
    );

    let (code, message) = api_message!(cfn.cancel_update_stack().stack_name(stack).send().await);
    assert_eq!(code, "ValidationError");
    assert_eq!(
        message,
        "CancelUpdateStack cannot be called from current stack status"
    );
    assert_eq!(events(&cfn, stack).await.len(), count);

    // A change set that changes nothing is created, and cannot run.
    cfn.create_change_set()
        .stack_name(stack)
        .change_set_name("nothing")
        .template_body(&body)
        .send()
        .await
        .expect("created");
    let described = cfn
        .describe_change_set()
        .stack_name(stack)
        .change_set_name("nothing")
        .send()
        .await
        .unwrap();
    assert_eq!(described.status().unwrap().as_str(), "FAILED");
    assert_eq!(
        described.execution_status().unwrap().as_str(),
        "UNAVAILABLE"
    );
    assert_eq!(
        described.status_reason(),
        Some("The submitted information didn't contain changes. Submit different information to create a change set.")
    );
}

/// Recorded: an export another stack imports can be neither removed nor changed,
/// with no resource touched; and deleting its stack does not take effect.
#[tokio::test]
async fn an_export_in_use_blocks_its_removal_change_and_stack_delete() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let exporting = |value: Option<&str>| {
        let mut template = json!({"Resources": {"Q": queue(30)}});
        if let Some(value) = value {
            template["Outputs"] = json!({"O": {"Value": value, "Export": {"Name": "cfn-in-use"}}});
        }
        template.to_string()
    };
    let stack_id = create(&cfn, "cfn-expa", &exporting(Some("x"))).await;
    create(
        &cfn,
        "cfn-expb",
        &resources(json!({"Q": {
            "Type": "AWS::SQS::Queue",
            "Properties": {"QueueName": {
                "Fn::Join": ["", [{"Fn::ImportValue": "cfn-in-use"}, "-q"]]
            }}
        }})),
    )
    .await;

    for (template, reason) in [
        (
            exporting(None),
            "Delete canceled. Cannot delete export cfn-in-use as it is in use by cfn-expb.",
        ),
        (
            exporting(Some("y")),
            "Update canceled. Cannot update export cfn-in-use as it is in use by cfn-expb.",
        ),
    ] {
        update(&cfn, "cfn-expa", &template).await;
        assert_eq!(
            last_operation(&cfn, "cfn-expa").await,
            vec![
                row(
                    "cfn-expa",
                    "UPDATE_IN_PROGRESS",
                    &stack_id,
                    "User Initiated"
                ),
                row("cfn-expa", "UPDATE_ROLLBACK_IN_PROGRESS", &stack_id, reason),
                row(
                    "cfn-expa",
                    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                    &stack_id,
                    ""
                ),
                row("cfn-expa", "UPDATE_ROLLBACK_COMPLETE", &stack_id, ""),
            ]
        );
    }

    cfn.delete_stack()
        .stack_name("cfn-expa")
        .send()
        .await
        .unwrap();
    assert_eq!(status(&cfn, "cfn-expa").await, "UPDATE_ROLLBACK_COMPLETE");

    // Free once the importer is gone.
    cfn.delete_stack()
        .stack_name("cfn-expb")
        .send()
        .await
        .unwrap();
    cfn.delete_stack()
        .stack_name("cfn-expa")
        .send()
        .await
        .unwrap();
    assert!(cfn
        .describe_stacks()
        .stack_name("cfn-expa")
        .send()
        .await
        .is_err());
}

/// Recorded: CreateStack accepts a literal export collision, then rolls back
/// without creating anything.
#[tokio::test]
async fn create_stack_export_collision_rolls_back_before_creating() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let sqs = server.sqs_client().await;
    let body = |queue_name: &str| {
        json!({
            "Resources": {"Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": queue_name}}},
            "Outputs": {"O": {"Value": "x", "Export": {"Name": "cfn-taken"}}}
        })
        .to_string()
    };
    create(&cfn, "cfn-owner", &body("cfn-owner-q")).await;
    let out = cfn
        .create_stack()
        .stack_name("cfn-late")
        .template_body(body("cfn-late-q"))
        .send()
        .await
        .expect("CreateStack succeeds");
    let stack_id = out.stack_id().unwrap().to_string();
    assert_eq!(
        last_operation(&cfn, "cfn-late").await,
        vec![
            row("cfn-late", "CREATE_IN_PROGRESS", &stack_id, "User Initiated"),
            row(
                "cfn-late",
                "ROLLBACK_IN_PROGRESS",
                &stack_id,
                "Export with name cfn-taken is already exported by stack cfn-owner. Rollback requested by user."
            ),
            row("cfn-late", "ROLLBACK_COMPLETE", &stack_id, ""),
        ]
    );
    let described = describe(&cfn, "cfn-late").await;
    assert_eq!(described.stack_status_reason(), None);
    assert!(sqs
        .get_queue_url()
        .queue_name("cfn-late-q")
        .send()
        .await
        .is_err());
}

/// Recorded: an unnamed task definition's replacement gets a new family, and the
/// old one's revision goes.
#[tokio::test]
async fn unnamed_task_definition_replacement_gets_a_new_family() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let definition = |cpu: u32| {
        json!({"D": {"Type": "AWS::ECS::TaskDefinition", "Properties": {
            "ContainerDefinitions": [{"Name": "c", "Image": "busybox", "Memory": 128, "Cpu": cpu}]
        }}})
    };
    create(&cfn, "cfn-td", &resources(definition(10))).await;
    let old = physical_ids(&cfn, "cfn-td").await[0].1.clone();
    update(&cfn, "cfn-td", &resources(definition(20))).await;
    assert_eq!(status(&cfn, "cfn-td").await, "UPDATE_COMPLETE");
    let new = physical_ids(&cfn, "cfn-td").await[0].1.clone();
    let family = |arn: &str| {
        arn.rsplit('/')
            .next()
            .unwrap()
            .rsplit_once(':')
            .unwrap()
            .0
            .to_string()
    };
    assert_ne!(family(&old), family(&new));
    assert!(new.ends_with(":1"), "{new}");
    assert!(family(&new).starts_with("cfn-td-D-"), "{new}");
    let rows = last_operation(&cfn, "cfn-td").await;
    assert!(rows.iter().any(|r| r.1 == "DELETE_COMPLETE" && r.2 == old));
}

/// A nested child's export in use holds its parent's delete, as the recorded
/// in-use delete does: accepted, and the stack stays.
#[tokio::test]
async fn a_nested_childs_export_in_use_blocks_the_parents_delete() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let child = json!({
        "Resources": {"Inner": queue(30)},
        "Outputs": {"O": {"Value": "x", "Export": {"Name": "cfn-kid-export"}}}
    })
    .to_string();
    create(
        &cfn,
        "cfn-family",
        &resources(json!({"Child": {
            "Type": "AWS::CloudFormation::Stack",
            "Properties": {"TemplateBody": child}
        }})),
    )
    .await;
    create(
        &cfn,
        "cfn-outsider",
        &resources(json!({"Q": {
            "Type": "AWS::SQS::Queue",
            "Properties": {"QueueName": {
                "Fn::Join": ["", [{"Fn::ImportValue": "cfn-kid-export"}, "-q"]]
            }}
        }})),
    )
    .await;

    cfn.delete_stack()
        .stack_name("cfn-family")
        .send()
        .await
        .unwrap();
    assert_eq!(status(&cfn, "cfn-family").await, "CREATE_COMPLETE");

    cfn.delete_stack()
        .stack_name("cfn-outsider")
        .send()
        .await
        .unwrap();
    cfn.delete_stack()
        .stack_name("cfn-family")
        .send()
        .await
        .unwrap();
    assert!(cfn
        .describe_stacks()
        .stack_name("cfn-family")
        .send()
        .await
        .is_err());
}

/// Rewrite one stack in the on-disk snapshot, as a crash at a chosen moment of its
/// cleanup would have left it.
fn edit_snapshot(dir: &std::path::Path, stack: &str, edit: impl Fn(&mut Value)) {
    fn find_stack<'a>(value: &'a mut Value, stack: &str) -> Option<&'a mut Value> {
        if value.get("name").and_then(Value::as_str) == Some(stack)
            && value.get("stack_id").is_some()
        {
            return Some(value);
        }
        match value {
            Value::Object(members) => members.values_mut().find_map(|v| find_stack(v, stack)),
            Value::Array(items) => items.iter_mut().find_map(|v| find_stack(v, stack)),
            _ => None,
        }
    }
    fn snapshot_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                snapshot_files(&path, out);
            } else if path.to_string_lossy().contains("cloudformation") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    snapshot_files(dir, &mut files);
    let file = files.first().expect("a cloudformation snapshot");
    let mut snapshot: Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let found = find_stack(&mut snapshot, stack).expect("the stack is in the snapshot");
    edit(found);
    std::fs::write(file, serde_json::to_vec(&snapshot).unwrap()).unwrap();
}

/// Through a real restart: a stack the crash caught in its cleanup finishes
/// deleting what it had left, whichever way the update was going.
#[tokio::test]
async fn a_restart_finishes_an_interrupted_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cfn = server.cloudformation_client().await;

    // Held stacks have a journal on disk to shape into a cleanup in progress.
    let held = |extra: &str| {
        resources(json!({
            "Keep": queue(30),
            extra: {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": format!("cfn-rs-{extra}")}},
            "Bad": {
                "Type": "AWS::SQS::Queue",
                "DependsOn": ["Keep", extra],
                "Properties": {"QueueName": "not a valid name!"}
            }
        }))
    };
    let base = resources(json!({"Keep": queue(30)}));
    for (stack, extra) in [("cfn-rs-update", "Doomed"), ("cfn-rs-rollback", "Added")] {
        create(&cfn, stack, &base).await;
        cfn.update_stack()
            .stack_name(stack)
            .template_body(held(extra))
            .disable_rollback(true)
            .send()
            .await
            .unwrap();
        assert_eq!(status(&cfn, stack).await, "UPDATE_FAILED");
    }
    let sns = server.sns_client().await;
    let topic_arn = |id: &str| format!("arn:aws:sns:us-east-1:123456789012:cfn-rs-{id}");
    sns.get_topic_attributes()
        .topic_arn(topic_arn("Doomed"))
        .send()
        .await
        .expect("provisioned");

    drop(cfn);
    let dir = tmp.path().to_path_buf();
    server
        .restart_after(|| {
            edit_snapshot(&dir, "cfn-rs-update", |stack| {
                stack["status"] = json!("UPDATE_COMPLETE_CLEANUP_IN_PROGRESS");
                let doomed = stack["resources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["logical_id"] == "Doomed")
                    .unwrap()
                    .clone();
                stack["pending_update"]["removed"] = json!([doomed]);
                stack["pending_update"]["entries"] = json!([]);
                stack["resources"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|r| r["logical_id"] != "Bad");
                stack["pending_update"]["applied"] = json!({});
            });
            edit_snapshot(&dir, "cfn-rs-rollback", |stack| {
                stack["status"] = json!("UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS");
                let added = stack["resources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["logical_id"] == "Added")
                    .unwrap()
                    .clone();
                stack["pending_update"]["rollback_deletes"] =
                    json!([{"Created": {"resource": added, "released": false}}]);
                stack["pending_update"]["entries"] = json!([]);
            });
        })
        .await;

    let cfn = server.cloudformation_client().await;
    let sns = server.sns_client().await;
    assert_eq!(status(&cfn, "cfn-rs-update").await, "UPDATE_COMPLETE");
    assert_eq!(
        status(&cfn, "cfn-rs-rollback").await,
        "UPDATE_ROLLBACK_COMPLETE"
    );
    assert!(sns
        .get_topic_attributes()
        .topic_arn(topic_arn("Doomed"))
        .send()
        .await
        .is_err());
    assert!(sns
        .get_topic_attributes()
        .topic_arn(topic_arn("Added"))
        .send()
        .await
        .is_err());
    let rolled_back = physical_ids(&cfn, "cfn-rs-rollback").await;
    assert_eq!(rolled_back.len(), 1, "{rolled_back:?}");
    assert_eq!(rolled_back[0].0, "Keep");

    // Both stay finished across another restart.
    drop(cfn);
    server.restart().await;
    let cfn = server.cloudformation_client().await;
    assert_eq!(status(&cfn, "cfn-rs-update").await, "UPDATE_COMPLETE");
}
