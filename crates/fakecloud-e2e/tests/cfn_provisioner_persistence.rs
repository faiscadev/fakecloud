//! Regression test for the CloudFormation provisioner persistence bug: with
//! `--storage-mode persistent`, resources created by the CFN provisioner were
//! never written through to disk and silently vanished on restart, while the
//! stack itself stayed CREATE_COMPLETE. The same resources created via their
//! direct service APIs persisted correctly.
//!
//! Here we create a Lambda function, a Secret, an SQS queue and an S3 bucket
//! BOTH via CloudFormation AND via the direct service APIs, restart the server
//! against the same data dir, and assert all eight survive. The CFN-created
//! lambda + secret are the deterministic regression guards (their services are
//! not re-mutated by normal use); the API-created set is the control. A second
//! test asserts a CFN-deleted resource stays gone after a restart.

mod helpers;

use helpers::TestServer;

const CFN_TEMPLATE: &str = r#"{"Resources":{
  "B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-bucket"}},
  "Sec":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"cfn-secret","SecretString":"s"}},
  "Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-queue"}},
  "F":{"Type":"AWS::Lambda::Function","Properties":{
    "FunctionName":"cfn-fn","Runtime":"provided.al2","Handler":"index.handler",
    "Role":"arn:aws:iam::123456789012:role/r",
    "Code":{"ZipFile":"def handler(event, context):\n    return {}\n"}}},
  "Zone":{"Type":"AWS::Route53::HostedZone","Properties":{"Name":"cfn-zone.example.com."}},
  "Vpc":{"Type":"AWS::EC2::VPC","Properties":{
    "CidrBlock":"10.42.0.0/16","Tags":[{"Key":"Name","Value":"cfn-vpc"}]}},
  "Sg":{"Type":"AWS::EC2::SecurityGroup","Properties":{
    "GroupName":"cfn-sg","GroupDescription":"cfn sg","VpcId":{"Ref":"Vpc"}}},
  "GlueDb":{"Type":"AWS::Glue::Database","Properties":{
    "CatalogId":"123456789012",
    "DatabaseInput":{"Name":"cfn_glue_db"}}}}}"#;

fn minimal_zip() -> Vec<u8> {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        zip.start_file("index.sh", opts).unwrap();
        zip.write_all(b"#!/bin/sh\necho hi\n").unwrap();
        zip.finish().unwrap();
    }
    buf
}

async fn lambda_names(server: &TestServer) -> Vec<String> {
    server
        .lambda_client()
        .await
        .list_functions()
        .send()
        .await
        .unwrap()
        .functions()
        .iter()
        .filter_map(|f| f.function_name().map(String::from))
        .collect()
}

async fn secret_names(server: &TestServer) -> Vec<String> {
    server
        .secretsmanager_client()
        .await
        .list_secrets()
        .send()
        .await
        .unwrap()
        .secret_list()
        .iter()
        .filter_map(|s| s.name().map(String::from))
        .collect()
}

async fn queue_urls(server: &TestServer) -> Vec<String> {
    server
        .sqs_client()
        .await
        .list_queues()
        .send()
        .await
        .unwrap()
        .queue_urls()
        .to_vec()
}

async fn hosted_zone_names(server: &TestServer) -> Vec<String> {
    server
        .route53_client()
        .await
        .list_hosted_zones()
        .send()
        .await
        .unwrap()
        .hosted_zones()
        .iter()
        .map(|z| z.name().to_string())
        .collect()
}

async fn glue_database_names(server: &TestServer) -> Vec<String> {
    server
        .glue_client()
        .await
        .get_databases()
        .send()
        .await
        .unwrap()
        .database_list()
        .iter()
        .map(|d| d.name().to_string())
        .collect()
}

async fn ec2_sg_names(server: &TestServer) -> Vec<String> {
    server
        .ec2_client()
        .await
        .describe_security_groups()
        .send()
        .await
        .unwrap()
        .security_groups()
        .iter()
        .map(|g| g.group_name().unwrap_or_default().to_string())
        .collect()
}

async fn ec2_vpc_cidrs(server: &TestServer) -> Vec<String> {
    server
        .ec2_client()
        .await
        .describe_vpcs()
        .send()
        .await
        .unwrap()
        .vpcs()
        .iter()
        .map(|v| v.cidr_block().unwrap_or_default().to_string())
        .collect()
}

async fn bucket_names(server: &TestServer) -> Vec<String> {
    server
        .s3_client()
        .await
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(String::from))
        .collect()
}

#[tokio::test]
async fn cfn_provisioned_resources_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;

    // --- create resources via CloudFormation ---
    let cf = server.cloudformation_client().await;
    cf.create_stack()
        .stack_name("probe")
        .template_body(CFN_TEMPLATE)
        .send()
        .await
        .expect("create_stack");
    let described = cf
        .describe_stacks()
        .stack_name("probe")
        .send()
        .await
        .expect("describe_stacks");
    assert_eq!(
        described
            .stacks()
            .first()
            .unwrap()
            .stack_status()
            .unwrap()
            .as_str(),
        "CREATE_COMPLETE"
    );

    // --- create equivalents via the direct service APIs (control set) ---
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("api-bucket")
        .send()
        .await
        .expect("create api-bucket");
    server
        .secretsmanager_client()
        .await
        .create_secret()
        .name("api-secret")
        .secret_string("s")
        .send()
        .await
        .expect("create api-secret");
    server
        .sqs_client()
        .await
        .create_queue()
        .queue_name("api-queue")
        .send()
        .await
        .expect("create api-queue");
    server
        .lambda_client()
        .await
        .create_function()
        .function_name("api-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Provided)
        .role("arn:aws:iam::123456789012:role/r")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(minimal_zip()))
                .build(),
        )
        .send()
        .await
        .expect("create api-fn");

    // --- restart against the same data dir ---
    server.restart().await;

    // --- all eight resources must survive ---
    let functions = lambda_names(&server).await;
    assert!(
        functions.contains(&"cfn-fn".to_string()),
        "cfn-fn lost: {functions:?}"
    );
    assert!(
        functions.contains(&"api-fn".to_string()),
        "api-fn lost: {functions:?}"
    );

    let secrets = secret_names(&server).await;
    assert!(
        secrets.contains(&"cfn-secret".to_string()),
        "cfn-secret lost: {secrets:?}"
    );
    assert!(
        secrets.contains(&"api-secret".to_string()),
        "api-secret lost: {secrets:?}"
    );

    let queues = queue_urls(&server).await;
    assert!(
        queues.iter().any(|u| u.ends_with("cfn-queue")),
        "cfn-queue lost: {queues:?}"
    );
    assert!(
        queues.iter().any(|u| u.ends_with("api-queue")),
        "api-queue lost: {queues:?}"
    );

    let buckets = bucket_names(&server).await;
    assert!(
        buckets.contains(&"cfn-bucket".to_string()),
        "cfn-bucket lost: {buckets:?}"
    );
    assert!(
        buckets.contains(&"api-bucket".to_string()),
        "api-bucket lost: {buckets:?}"
    );

    // Services whose CFN namespace differs from the fakecloud service name and
    // were missing from `service_key_for_type` (#1766 class): these vanished on
    // restart before the fix.
    let zones = hosted_zone_names(&server).await;
    assert!(
        zones.iter().any(|n| n == "cfn-zone.example.com."),
        "cfn-zone lost: {zones:?}"
    );
    let glue_dbs = glue_database_names(&server).await;
    assert!(
        glue_dbs.contains(&"cfn_glue_db".to_string()),
        "cfn_glue_db lost: {glue_dbs:?}"
    );

    // EC2 was wired into the provisioner (2026-06-25 #1957) but missing from
    // `service_key_for_type`, so CFN-created VPC/SG state vanished on restart
    // while direct-API EC2 resources persisted (#1766 class).
    let sgs = ec2_sg_names(&server).await;
    assert!(sgs.contains(&"cfn-sg".to_string()), "cfn-sg lost: {sgs:?}");
    let cidrs = ec2_vpc_cidrs(&server).await;
    assert!(
        cidrs.contains(&"10.42.0.0/16".to_string()),
        "cfn-vpc lost: {cidrs:?}"
    );
}

#[tokio::test]
async fn cfn_deleted_resources_stay_gone_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;

    let cf = server.cloudformation_client().await;
    cf.create_stack()
        .stack_name("probe")
        .template_body(CFN_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    // Sanity: the CFN lambda + bucket exist before delete.
    assert!(lambda_names(&server).await.contains(&"cfn-fn".to_string()));
    assert!(bucket_names(&server)
        .await
        .contains(&"cfn-bucket".to_string()));

    // Delete the stack, then restart against the same data dir.
    server
        .cloudformation_client()
        .await
        .delete_stack()
        .stack_name("probe")
        .send()
        .await
        .expect("delete_stack");
    server.restart().await;

    // The CFN-deleted resources must NOT reappear after restart.
    let functions = lambda_names(&server).await;
    assert!(
        !functions.contains(&"cfn-fn".to_string()),
        "cfn-fn reappeared: {functions:?}"
    );
    let buckets = bucket_names(&server).await;
    assert!(
        !buckets.contains(&"cfn-bucket".to_string()),
        "cfn-bucket reappeared: {buckets:?}"
    );
}
