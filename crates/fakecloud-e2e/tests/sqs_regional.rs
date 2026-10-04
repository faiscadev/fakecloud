//! SQS queues are regional resources: the same queue name exists
//! independently in every region of an account, `ListQueues` and
//! `GetQueueUrl` see only the request region, messages never cross regions,
//! and cross-service deliveries by queue ARN land in the ARN's region.
//! Persisted pre-regional snapshots load each queue into its ARN's region.

mod helpers;

use aws_sdk_sqs::types::QueueAttributeName;
use helpers::TestServer;

async fn sqs_in(server: &TestServer, region: &str) -> aws_sdk_sqs::Client {
    aws_sdk_sqs::Client::new(&server.aws_config_in(region).await)
}

async fn queue_arn(sqs: &aws_sdk_sqs::Client, url: &str) -> String {
    sqs.get_queue_attributes()
        .queue_url(url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("GetQueueAttributes")
        .attributes
        .unwrap()
        .remove(&QueueAttributeName::QueueArn)
        .unwrap()
}

async fn receive_bodies(sqs: &aws_sdk_sqs::Client, url: &str) -> Vec<String> {
    sqs.receive_message()
        .queue_url(url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("ReceiveMessage")
        .messages()
        .iter()
        .filter_map(|m| m.body().map(str::to_string))
        .collect()
}

async fn list_names(sqs: &aws_sdk_sqs::Client) -> Vec<String> {
    let mut names: Vec<String> = sqs
        .list_queues()
        .send()
        .await
        .expect("ListQueues")
        .queue_urls()
        .iter()
        .map(|u| u.rsplit('/').next().unwrap().to_string())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn same_queue_name_coexists_in_two_regions() {
    let server = TestServer::start().await;
    let east = sqs_in(&server, "us-east-1").await;
    let west = sqs_in(&server, "eu-west-1").await;

    let east_url = east
        .create_queue()
        .queue_name("orders")
        .attributes(QueueAttributeName::VisibilityTimeout, "40")
        .send()
        .await
        .expect("create east")
        .queue_url
        .unwrap();
    let west_url = west
        .create_queue()
        .queue_name("orders")
        .attributes(QueueAttributeName::VisibilityTimeout, "50")
        .send()
        .await
        .expect("create west")
        .queue_url
        .unwrap();

    // Each region's queue has its own ARN and attributes.
    assert!(queue_arn(&east, &east_url).await.contains(":us-east-1:"));
    assert!(queue_arn(&west, &west_url).await.contains(":eu-west-1:"));
    let vt = |c: aws_sdk_sqs::Client, u: String| async move {
        c.get_queue_attributes()
            .queue_url(u)
            .attribute_names(QueueAttributeName::VisibilityTimeout)
            .send()
            .await
            .unwrap()
            .attributes
            .unwrap()
            .remove(&QueueAttributeName::VisibilityTimeout)
            .unwrap()
    };
    assert_eq!(vt(east.clone(), east_url.clone()).await, "40");
    assert_eq!(vt(west.clone(), west_url.clone()).await, "50");

    // Lists and GetQueueUrl are region-scoped.
    west.create_queue()
        .queue_name("west-only")
        .send()
        .await
        .unwrap();
    assert_eq!(list_names(&east).await, ["orders"]);
    assert_eq!(list_names(&west).await, ["orders", "west-only"]);
    assert!(east
        .get_queue_url()
        .queue_name("west-only")
        .send()
        .await
        .is_err());

    // Messages stay in their region's queue.
    west.send_message()
        .queue_url(&west_url)
        .message_body("hello-west")
        .send()
        .await
        .unwrap();
    assert!(receive_bodies(&east, &east_url).await.is_empty());
    assert_eq!(receive_bodies(&west, &west_url).await, ["hello-west"]);

    // Deleting one region's queue leaves the other's.
    west.delete_queue()
        .queue_url(&west_url)
        .send()
        .await
        .unwrap();
    assert_eq!(list_names(&east).await, ["orders"]);
    assert_eq!(list_names(&west).await, ["west-only"]);
}

#[tokio::test]
async fn sns_fanout_delivers_to_the_queue_in_its_arn_region() {
    let server = TestServer::start().await;
    let east = sqs_in(&server, "us-east-1").await;
    let west = sqs_in(&server, "eu-west-1").await;
    let sns_west = aws_sdk_sns::Client::new(&server.aws_config_in("eu-west-1").await);

    let east_url = east
        .create_queue()
        .queue_name("fanout")
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let west_url = west
        .create_queue()
        .queue_name("fanout")
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let west_arn = queue_arn(&west, &west_url).await;

    let topic_arn = sns_west
        .create_topic()
        .name("events")
        .send()
        .await
        .unwrap()
        .topic_arn
        .unwrap();
    sns_west
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&west_arn)
        .attributes("RawMessageDelivery", "true")
        .send()
        .await
        .unwrap();
    sns_west
        .publish()
        .topic_arn(&topic_arn)
        .message("to-west")
        .send()
        .await
        .unwrap();

    assert_eq!(receive_bodies(&west, &west_url).await, ["to-west"]);
    assert!(receive_bodies(&east, &east_url).await.is_empty());
}

/// A v2 (pre-regional) snapshot kept every queue of an account in one map.
/// Loading it puts each queue in the region its ARN names.
#[tokio::test]
async fn legacy_snapshot_loads_queues_into_their_arn_region() {
    let tmp = tempfile::tempdir().unwrap();
    let account = "123456789012";
    let queue = |name: &str, region: &str| {
        serde_json::json!({
            "queue_name": name,
            "queue_url": format!("http://localhost:4566/{account}/{name}"),
            "arn": format!("arn:aws:sqs:{region}:{account}:{name}"),
            "created_at": "2026-01-01T00:00:00Z",
            "messages": [],
            "inflight": [],
            "attributes": {"VisibilityTimeout": "30", "DelaySeconds": "0"},
            "is_fifo": false,
            "dedup_cache": {},
            "redrive_policy": null,
            "tags": {},
            "next_sequence_number": 0,
            "permission_labels": [],
            "receipt_handle_map": {}
        })
    };
    let snapshot = serde_json::json!({
        "schema_version": 2,
        "accounts": {
            "default_account_id": account,
            "region": "us-east-1",
            "endpoint": "http://localhost:4566",
            "accounts": {
                account: {
                    "account_id": account,
                    "region": "us-east-1",
                    "endpoint": "http://localhost:4566",
                    "queues": {
                        format!("http://localhost:4566/{account}/legacy-east"): queue("legacy-east", "us-east-1"),
                        format!("http://localhost:4566/{account}/legacy-west"): queue("legacy-west", "eu-west-1"),
                    },
                    "name_to_url": {
                        "legacy-east": format!("http://localhost:4566/{account}/legacy-east"),
                        "legacy-west": format!("http://localhost:4566/{account}/legacy-west"),
                    }
                }
            }
        }
    });
    // Let a server initialize the data directory (version file), then plant
    // the legacy snapshot before the next start.
    drop(TestServer::start_persistent(tmp.path()).await);
    std::fs::create_dir_all(tmp.path().join("sqs")).unwrap();
    std::fs::write(
        tmp.path().join("sqs").join("snapshot.json"),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();

    let server = TestServer::start_persistent(tmp.path()).await;
    let east = sqs_in(&server, "us-east-1").await;
    let west = sqs_in(&server, "eu-west-1").await;
    assert_eq!(list_names(&east).await, ["legacy-east"]);
    assert_eq!(list_names(&west).await, ["legacy-west"]);
    let url = west
        .get_queue_url()
        .queue_name("legacy-west")
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    assert_eq!(
        queue_arn(&west, &url).await,
        format!("arn:aws:sqs:eu-west-1:{account}:legacy-west")
    );
}

#[tokio::test]
async fn regional_queues_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    for region in ["us-east-1", "ap-southeast-2"] {
        sqs_in(&server, region)
            .await
            .create_queue()
            .queue_name(format!("q-{region}"))
            .send()
            .await
            .unwrap();
    }
    server.restart().await;
    assert_eq!(
        list_names(&sqs_in(&server, "us-east-1").await).await,
        ["q-us-east-1"]
    );
    assert_eq!(
        list_names(&sqs_in(&server, "ap-southeast-2").await).await,
        ["q-ap-southeast-2"]
    );
}
