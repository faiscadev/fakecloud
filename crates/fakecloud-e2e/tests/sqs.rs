mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn sqs_create_list_delete_queue() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create
    let resp = client
        .create_queue()
        .queue_name("test-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();
    assert!(queue_url.contains("test-queue"));

    // List
    let resp = client.list_queues().send().await.unwrap();
    let urls = resp.queue_urls();
    assert_eq!(urls.len(), 1);
    assert_eq!(urls[0], queue_url);

    // Delete
    client
        .delete_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();

    let resp = client.list_queues().send().await.unwrap();
    assert_eq!(resp.queue_urls().len(), 0);
}

#[tokio::test]
async fn sqs_send_receive_delete_message() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("msg-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send
    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("hello world")
        .send()
        .await
        .unwrap();

    // Receive
    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    let messages = resp.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body().unwrap(), "hello world");

    let receipt_handle = messages[0].receipt_handle().unwrap().to_string();

    // Delete message
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(&receipt_handle)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sqs_multiple_messages() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("multi-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    for i in 0..5 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("message {i}"))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.messages().len(), 5);
}

#[tokio::test]
async fn sqs_get_queue_url() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let create_resp = client
        .create_queue()
        .queue_name("url-queue")
        .send()
        .await
        .unwrap();
    let expected_url = create_resp.queue_url().unwrap();

    let resp = client
        .get_queue_url()
        .queue_name("url-queue")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.queue_url().unwrap(), expected_url);
}

#[tokio::test]
async fn sqs_purge_queue() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("purge-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("to be purged")
        .send()
        .await
        .unwrap();

    client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();

    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.messages().len(), 0);
}

#[tokio::test]
async fn sqs_create_queue_idempotent() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp1 = client
        .create_queue()
        .queue_name("idempotent-queue")
        .send()
        .await
        .unwrap();
    let resp2 = client
        .create_queue()
        .queue_name("idempotent-queue")
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.queue_url().unwrap(), resp2.queue_url().unwrap());
}

#[tokio::test]
async fn sqs_cli_create_and_list() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["sqs", "create-queue", "--queue-name", "cli-queue"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    let output = server.aws_cli(&["sqs", "list-queues"]).await;
    assert!(output.success(), "list failed: {}", output.stderr_text());
    let json = output.stdout_json();
    let urls = json["QueueUrls"].as_array().unwrap();
    assert_eq!(urls.len(), 1);
    assert!(urls[0].as_str().unwrap().contains("cli-queue"));
}
