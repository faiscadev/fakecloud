mod helpers;

use std::io::Write;

use aws_sdk_lambda::primitives::Blob;
use helpers::TestServer;

fn make_python_zip() -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();
    writer.start_file("index.py", options).unwrap();
    writer
        .write_all(b"def handler(event, context):\n    return {\"statusCode\": 200}\n")
        .unwrap();
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

/// Function + event source mapping survive a restart.
#[tokio::test]
async fn persistence_round_trip_function_and_esm() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("persist-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // The mapping's source must exist.
    let sqs = server.sqs_client().await;
    let queue_url = sqs
        .create_queue()
        .queue_name("persist-queue")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()[&aws_sdk_sqs::types::QueueAttributeName::QueueArn]
        .clone();
    let esm = client
        .create_event_source_mapping()
        .function_name("persist-func")
        .event_source_arn(&queue_arn)
        .batch_size(5)
        .send()
        .await
        .unwrap();
    let esm_uuid = esm.uuid().unwrap().to_string();

    drop(client);
    server.restart().await;
    let client = server.lambda_client().await;

    // Function survived.
    let func = client
        .get_function()
        .function_name("persist-func")
        .send()
        .await
        .unwrap();
    let config = func.configuration().unwrap();
    assert_eq!(config.function_name().unwrap(), "persist-func");
    assert_eq!(config.runtime().unwrap().as_str(), "python3.12");

    // ESM survived.
    let esm_get = client
        .get_event_source_mapping()
        .uuid(&esm_uuid)
        .send()
        .await
        .unwrap();
    assert_eq!(esm_get.batch_size(), Some(5));

    // ListFunctions returns the function.
    let listed = client.list_functions().send().await.unwrap();
    assert!(listed
        .functions()
        .iter()
        .any(|f| f.function_name() == Some("persist-func")));
}

/// Deletion survives a restart.
#[tokio::test]
async fn persistence_deletion_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("doomed-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_python_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .delete_function()
        .function_name("doomed-func")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.lambda_client().await;

    let listed = client.list_functions().send().await.unwrap();
    assert!(!listed
        .functions()
        .iter()
        .any(|f| f.function_name() == Some("doomed-func")));
}
