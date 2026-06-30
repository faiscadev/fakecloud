//! Amazon EventBridge Pipes control plane: CreatePipe -> (settle to RUNNING)
//! -> Describe -> List/filter -> Stop -> Start -> Update -> tags -> Delete.
//! Real source->enrichment->target execution lands in a later batch.

mod helpers;

use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::000000000000:role/pipe-role";

async fn wait_for_state(
    pipes: &aws_sdk_pipes::Client,
    name: &str,
    want: aws_sdk_pipes::types::PipeState,
    max_secs: u64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = pipes.describe_pipe().name(name).send().await {
            if resp.current_state() == Some(&want) {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("pipe {name} did not reach {want:?} within {max_secs}s");
}

#[tokio::test]
async fn pipes_control_plane_end_to_end() {
    let s = TestServer::start().await;
    let pipes = aws_sdk_pipes::Client::new(&s.aws_config().await);

    let source = "arn:aws:sqs:us-east-1:000000000000:src-queue";
    let target = "arn:aws:sqs:us-east-1:000000000000:dst-queue";

    // CreatePipe returns CREATING, then settles to RUNNING.
    let created = pipes
        .create_pipe()
        .name("orders-pipe")
        .source(source)
        .target(target)
        .role_arn(ROLE)
        .send()
        .await
        .expect("create pipe");
    assert_eq!(created.name(), Some("orders-pipe"));
    assert_eq!(
        created.current_state(),
        Some(&aws_sdk_pipes::types::PipeState::Creating)
    );
    assert!(created.arn().unwrap().contains(":pipe/orders-pipe"));

    wait_for_state(
        &pipes,
        "orders-pipe",
        aws_sdk_pipes::types::PipeState::Running,
        5,
    )
    .await;

    // Describe echoes source/target/role back.
    let d = pipes
        .describe_pipe()
        .name("orders-pipe")
        .send()
        .await
        .unwrap();
    assert_eq!(d.source(), Some(source));
    assert_eq!(d.target(), Some(target));
    assert_eq!(d.role_arn(), Some(ROLE));
    assert_eq!(
        d.desired_state(),
        Some(&aws_sdk_pipes::types::RequestedPipeStateDescribeResponse::Running)
    );

    // A second pipe, born STOPPED.
    pipes
        .create_pipe()
        .name("audit-pipe")
        .source(source)
        .target(target)
        .role_arn(ROLE)
        .desired_state(aws_sdk_pipes::types::RequestedPipeState::Stopped)
        .send()
        .await
        .expect("create stopped pipe");
    wait_for_state(
        &pipes,
        "audit-pipe",
        aws_sdk_pipes::types::PipeState::Stopped,
        5,
    )
    .await;

    // ListPipes + NamePrefix filter.
    let all = pipes.list_pipes().send().await.unwrap();
    assert_eq!(all.pipes().len(), 2);
    let filtered = pipes
        .list_pipes()
        .name_prefix("orders")
        .send()
        .await
        .unwrap();
    assert_eq!(filtered.pipes().len(), 1);
    assert_eq!(filtered.pipes()[0].name(), Some("orders-pipe"));

    // CurrentState filter.
    let stopped = pipes
        .list_pipes()
        .current_state(aws_sdk_pipes::types::PipeState::Stopped)
        .send()
        .await
        .unwrap();
    assert_eq!(stopped.pipes().len(), 1);
    assert_eq!(stopped.pipes()[0].name(), Some("audit-pipe"));

    // Stop then Start the running pipe.
    pipes.stop_pipe().name("orders-pipe").send().await.unwrap();
    wait_for_state(
        &pipes,
        "orders-pipe",
        aws_sdk_pipes::types::PipeState::Stopped,
        5,
    )
    .await;
    pipes.start_pipe().name("orders-pipe").send().await.unwrap();
    wait_for_state(
        &pipes,
        "orders-pipe",
        aws_sdk_pipes::types::PipeState::Running,
        5,
    )
    .await;

    // UpdatePipe changes the description.
    pipes
        .update_pipe()
        .name("orders-pipe")
        .role_arn(ROLE)
        .description("order ingestion pipe")
        .send()
        .await
        .expect("update pipe");
    wait_for_state(
        &pipes,
        "orders-pipe",
        aws_sdk_pipes::types::PipeState::Running,
        5,
    )
    .await;
    let d = pipes
        .describe_pipe()
        .name("orders-pipe")
        .send()
        .await
        .unwrap();
    assert_eq!(d.description(), Some("order ingestion pipe"));

    // Tags round-trip.
    let arn = d.arn().unwrap().to_string();
    pipes
        .tag_resource()
        .resource_arn(&arn)
        .tags("env", "prod")
        .send()
        .await
        .expect("tag");
    let tags = pipes
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("prod")
    );
    pipes
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag");
    let tags = pipes
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().map(|t| t.is_empty()).unwrap_or(true));

    // DeletePipe transitions through DELETING then disappears.
    pipes
        .delete_pipe()
        .name("orders-pipe")
        .send()
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let r = pipes.describe_pipe().name("orders-pipe").send().await;
        if r.is_err() {
            break; // NotFound once settled
        }
        assert!(std::time::Instant::now() < deadline, "pipe never deleted");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // DescribePipe on a missing pipe is a NotFoundException.
    let err = pipes
        .describe_pipe()
        .name("does-not-exist")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("NotFound"));
}
