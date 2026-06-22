//! One `#[tokio::test]` per CI matrix shard. Each runs the `TestAcc*`
//! tests selected by its `Shard` (filtered by `run_regex` minus the
//! merged deny-list).
//!
//! Hard-fails if the `go` or `terraform` binaries are missing. Running
//! this crate is an opt-in signal that the caller wants the upstream
//! Terraform suite exercised — silently passing on a machine that can't
//! run it would just hide regressions.

use fakecloud_tfacc::{
    require_toolchain, setup_provider_source, GoTestRunner, Shard, TestServer, SHARDS,
};

async fn run_shard(name: &str) {
    require_toolchain();
    let shard: &Shard = SHARDS
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("shard `{name}` not in SHARDS list"));

    let provider_root = setup_provider_source().expect("setup terraform-provider-aws");
    let server = TestServer::start().await;
    let runner = GoTestRunner {
        provider_root: &provider_root,
        endpoint: server.endpoint(),
    };
    runner.run_shard(shard).assert_pass(name);
}

#[tokio::test]
async fn s3_acceptance() {
    run_shard("s3").await;
}

#[tokio::test]
async fn sts_acceptance() {
    run_shard("sts").await;
}

#[tokio::test]
async fn cognitoidp_acceptance() {
    run_shard("cognitoidp").await;
}

#[tokio::test]
async fn bedrock_acceptance() {
    run_shard("bedrock").await;
}

#[tokio::test]
async fn apigatewayv2_acceptance() {
    run_shard("apigatewayv2").await;
}

#[tokio::test]
async fn kinesis_acceptance() {
    run_shard("kinesis").await;
}

#[tokio::test]
async fn sns_acceptance() {
    run_shard("sns").await;
}

#[tokio::test]
async fn events_acceptance() {
    run_shard("events").await;
}

#[tokio::test]
async fn kms_acceptance() {
    run_shard("kms").await;
}

#[tokio::test]
async fn logs_acceptance() {
    run_shard("logs").await;
}

#[tokio::test]
async fn iam_acceptance() {
    run_shard("iam").await;
}

#[tokio::test]
async fn ssm_acceptance() {
    run_shard("ssm").await;
}

#[tokio::test]
async fn secretsmanager_acceptance() {
    run_shard("secretsmanager").await;
}

#[tokio::test]
async fn sqs_core_acceptance() {
    run_shard("sqs-core").await;
}

#[tokio::test]
async fn sqs_encryption_acceptance() {
    run_shard("sqs-encryption").await;
}

#[tokio::test]
async fn dynamodb_a_g_acceptance() {
    run_shard("dynamodb-a-g").await;
}

#[tokio::test]
async fn dynamodb_h_z_acceptance() {
    run_shard("dynamodb-h-z").await;
}

#[tokio::test]
async fn dynamodb_resources_acceptance() {
    run_shard("dynamodb-resources").await;
}
