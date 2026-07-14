//! Conformance coverage for AWS Config (`config`, awsJson1_1, target prefix
//! `StarlingDoveService`).
//!
//! There is no typed `aws-sdk-config` in this workspace, so every operation is
//! driven over raw HTTP with the `X-Amz-Target` header, mirroring the acm-pca
//! suite. One `#[test_action]` per `SUPPORTED_ACTIONS` entry pins the operation
//! to its Smithy checksum so model drift fails the build; the audit
//! cross-checks this list against the service crate.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;
use serde_json::{json, Value};

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/config/aws4_request, SignedHeaders=host, Signature=0";

/// POST an awsJson1_1 Config action, returning `(status, parsed_body)`.
async fn cfg(server: &TestServer, op: &str, body: Value) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-amz-json-1.1")
        .header("x-amz-target", format!("StarlingDoveService.{op}"))
        .header("Authorization", AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, parsed)
}

const ROLE_ARN: &str =
    "arn:aws:iam::123456789012:role/aws-service-role/config.amazonaws.com/AWSServiceRoleForConfig";

async fn make_recorder(server: &TestServer) {
    let (s, b) = cfg(
        server,
        "PutConfigurationRecorder",
        json!({ "ConfigurationRecorder": { "name": "default", "roleARN": ROLE_ARN } }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

async fn make_rule(server: &TestServer, name: &str) {
    let (s, b) = cfg(
        server,
        "PutConfigRule",
        json!({ "ConfigRule": { "ConfigRuleName": name, "Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" } } }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

async fn make_channel(server: &TestServer) {
    // Real AWS Config validates that the delivery channel's S3 bucket exists at
    // PutDeliveryChannel time (otherwise NoSuchBucketException), so create the
    // bucket first — the same precondition the operation requires against AWS.
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("config-bucket")
        .send()
        .await
        .unwrap();
    let (s, b) = cfg(
        server,
        "PutDeliveryChannel",
        json!({ "DeliveryChannel": { "name": "default", "s3BucketName": "config-bucket" } }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

async fn make_pack(server: &TestServer, name: &str) {
    let (s, b) = cfg(
        server,
        "PutConformancePack",
        json!({ "ConformancePackName": name, "TemplateBody": "Resources: {}" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

async fn make_aggregator(server: &TestServer, name: &str) {
    let (s, b) = cfg(
        server,
        "PutConfigurationAggregator",
        json!({ "ConfigurationAggregatorName": name, "AccountAggregationSources": [{ "AccountIds": ["123456789012"], "AllAwsRegions": true }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

async fn put_item(server: &TestServer) {
    let (s, b) = cfg(
        server,
        "PutResourceConfig",
        json!({
            "ResourceType": "AWS::EC2::CustomerGateway",
            "SchemaVersionId": "00000000",
            "ResourceId": "cgw-1234",
            "Configuration": "{\"k\":\"v\"}",
        }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Recorders ──────────────────────────────────────────────────────────────

#[test_action("config", "PutConfigurationRecorder", checksum = "c794e40f")]
#[tokio::test]
async fn put_configuration_recorder() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
}

#[test_action("config", "DescribeConfigurationRecorders", checksum = "0e28bf78")]
#[tokio::test]
async fn describe_configuration_recorders() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    let (s, b) = cfg(&server, "DescribeConfigurationRecorders", json!({})).await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["ConfigurationRecorders"][0]["name"], "default");
}

#[test_action("config", "DescribeConfigurationRecorderStatus", checksum = "66b70fe4")]
#[tokio::test]
async fn describe_configuration_recorder_status() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    let (s, b) = cfg(&server, "DescribeConfigurationRecorderStatus", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteConfigurationRecorder", checksum = "12e9fcf8")]
#[tokio::test]
async fn delete_configuration_recorder() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    let (s, b) = cfg(
        &server,
        "DeleteConfigurationRecorder",
        json!({ "ConfigurationRecorderName": "default" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "StartConfigurationRecorder", checksum = "a87752d3")]
#[tokio::test]
async fn start_configuration_recorder() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    let (s, b) = cfg(
        &server,
        "StartConfigurationRecorder",
        json!({ "ConfigurationRecorderName": "default" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "StopConfigurationRecorder", checksum = "f3445975")]
#[tokio::test]
async fn stop_configuration_recorder() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    cfg(
        &server,
        "StartConfigurationRecorder",
        json!({ "ConfigurationRecorderName": "default" }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "StopConfigurationRecorder",
        json!({ "ConfigurationRecorderName": "default" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "ListConfigurationRecorders", checksum = "b1d05f0b")]
#[tokio::test]
async fn list_configuration_recorders() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    let (s, b) = cfg(&server, "ListConfigurationRecorders", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "PutServiceLinkedConfigurationRecorder",
    checksum = "5757b1a4"
)]
#[tokio::test]
async fn put_service_linked_configuration_recorder() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "PutServiceLinkedConfigurationRecorder",
        json!({ "ServicePrincipal": "securityhub.amazonaws.com" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert!(b["Arn"].as_str().is_some());
}

#[test_action(
    "config",
    "DeleteServiceLinkedConfigurationRecorder",
    checksum = "09ae42f2"
)]
#[tokio::test]
async fn delete_service_linked_configuration_recorder() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutServiceLinkedConfigurationRecorder",
        json!({ "ServicePrincipal": "securityhub.amazonaws.com" }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "DeleteServiceLinkedConfigurationRecorder",
        json!({ "ServicePrincipal": "securityhub.amazonaws.com" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "AssociateResourceTypes", checksum = "55790e4d")]
#[tokio::test]
async fn associate_resource_types() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    let (s, b) = cfg(
        &server,
        "AssociateResourceTypes",
        json!({ "ConfigurationRecorderArn": "default", "ResourceTypes": ["AWS::S3::Bucket"] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DisassociateResourceTypes", checksum = "80b2e7ac")]
#[tokio::test]
async fn disassociate_resource_types() {
    let server = TestServer::start().await;
    make_recorder(&server).await;
    cfg(
        &server,
        "AssociateResourceTypes",
        json!({ "ConfigurationRecorderArn": "default", "ResourceTypes": ["AWS::S3::Bucket"] }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "DisassociateResourceTypes",
        json!({ "ConfigurationRecorderArn": "default", "ResourceTypes": ["AWS::S3::Bucket"] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Delivery channels ────────────────────────────────────────────────────────

#[test_action("config", "PutDeliveryChannel", checksum = "cc726548")]
#[tokio::test]
async fn put_delivery_channel() {
    let server = TestServer::start().await;
    make_channel(&server).await;
}

#[test_action("config", "DescribeDeliveryChannels", checksum = "cc8ed5c9")]
#[tokio::test]
async fn describe_delivery_channels() {
    let server = TestServer::start().await;
    make_channel(&server).await;
    let (s, b) = cfg(&server, "DescribeDeliveryChannels", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeDeliveryChannelStatus", checksum = "23d8d2a7")]
#[tokio::test]
async fn describe_delivery_channel_status() {
    let server = TestServer::start().await;
    make_channel(&server).await;
    let (s, b) = cfg(&server, "DescribeDeliveryChannelStatus", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteDeliveryChannel", checksum = "a8412ea4")]
#[tokio::test]
async fn delete_delivery_channel() {
    let server = TestServer::start().await;
    make_channel(&server).await;
    let (s, b) = cfg(
        &server,
        "DeleteDeliveryChannel",
        json!({ "DeliveryChannelName": "default" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeliverConfigSnapshot", checksum = "ac23bc58")]
#[tokio::test]
async fn deliver_config_snapshot() {
    let server = TestServer::start().await;
    make_channel(&server).await;
    let (s, b) = cfg(
        &server,
        "DeliverConfigSnapshot",
        json!({ "deliveryChannelName": "default" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert!(b["configSnapshotId"].as_str().is_some());
}

// ── Config items ─────────────────────────────────────────────────────────────

#[test_action("config", "PutResourceConfig", checksum = "8a096bf9")]
#[tokio::test]
async fn put_resource_config() {
    let server = TestServer::start().await;
    put_item(&server).await;
}

#[test_action("config", "DeleteResourceConfig", checksum = "12c5d382")]
#[tokio::test]
async fn delete_resource_config() {
    let server = TestServer::start().await;
    put_item(&server).await;
    let (s, b) = cfg(
        &server,
        "DeleteResourceConfig",
        json!({ "ResourceType": "AWS::EC2::CustomerGateway", "ResourceId": "cgw-1234" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetResourceConfigHistory", checksum = "2176ee43")]
#[tokio::test]
async fn get_resource_config_history() {
    let server = TestServer::start().await;
    put_item(&server).await;
    let (s, b) = cfg(
        &server,
        "GetResourceConfigHistory",
        json!({ "resourceType": "AWS::EC2::CustomerGateway", "resourceId": "cgw-1234" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert!(!b["configurationItems"].as_array().unwrap().is_empty());
}

#[test_action("config", "BatchGetResourceConfig", checksum = "1e9c036b")]
#[tokio::test]
async fn batch_get_resource_config() {
    let server = TestServer::start().await;
    put_item(&server).await;
    let (s, b) = cfg(&server, "BatchGetResourceConfig", json!({ "resourceKeys": [{ "resourceType": "AWS::EC2::CustomerGateway", "resourceId": "cgw-1234" }] })).await;
    assert_eq!(s, 200, "{b}");
    assert!(!b["baseConfigurationItems"].as_array().unwrap().is_empty());
}

#[test_action("config", "ListDiscoveredResources", checksum = "5efc7d9e")]
#[tokio::test]
async fn list_discovered_resources() {
    let server = TestServer::start().await;
    put_item(&server).await;
    let (s, b) = cfg(
        &server,
        "ListDiscoveredResources",
        json!({ "resourceType": "AWS::EC2::CustomerGateway" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetDiscoveredResourceCounts", checksum = "bb922049")]
#[tokio::test]
async fn get_discovered_resource_counts() {
    let server = TestServer::start().await;
    put_item(&server).await;
    let (s, b) = cfg(&server, "GetDiscoveredResourceCounts", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

// ── Config rules + evaluation ────────────────────────────────────────────────

#[test_action("config", "PutConfigRule", checksum = "080cf896")]
#[tokio::test]
async fn put_config_rule() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
}

#[test_action("config", "DescribeConfigRules", checksum = "e2100865")]
#[tokio::test]
async fn describe_config_rules() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(&server, "DescribeConfigRules", json!({})).await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["ConfigRules"][0]["ConfigRuleName"], "rule1");
}

#[test_action("config", "DeleteConfigRule", checksum = "b33d5697")]
#[tokio::test]
async fn delete_config_rule() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "DeleteConfigRule",
        json!({ "ConfigRuleName": "rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeConfigRuleEvaluationStatus", checksum = "0b94020f")]
#[tokio::test]
async fn describe_config_rule_evaluation_status() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(&server, "DescribeConfigRuleEvaluationStatus", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "StartConfigRulesEvaluation", checksum = "0cef922e")]
#[tokio::test]
async fn start_config_rules_evaluation() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "StartConfigRulesEvaluation",
        json!({ "ConfigRuleNames": ["rule1"] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutEvaluations", checksum = "fdb6b2eb")]
#[tokio::test]
async fn put_evaluations() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "PutEvaluations",
        json!({
            "ResultToken": "rule1#token",
            "Evaluations": [{ "ComplianceResourceType": "AWS::S3::Bucket", "ComplianceResourceId": "b1", "ComplianceType": "COMPLIANT", "OrderingTimestamp": 1700000000 }],
        }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert!(b["FailedEvaluations"].as_array().unwrap().is_empty());
}

#[test_action("config", "PutExternalEvaluation", checksum = "3910c55c")]
#[tokio::test]
async fn put_external_evaluation() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "PutExternalEvaluation",
        json!({
            "ConfigRuleName": "ext-rule",
            "ExternalEvaluation": { "ComplianceResourceType": "AWS::S3::Bucket", "ComplianceResourceId": "b1", "ComplianceType": "NON_COMPLIANT", "OrderingTimestamp": 1700000000 },
        }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteEvaluationResults", checksum = "bab07fd5")]
#[tokio::test]
async fn delete_evaluation_results() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "DeleteEvaluationResults",
        json!({ "ConfigRuleName": "rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetCustomRulePolicy", checksum = "05575e66")]
#[tokio::test]
async fn get_custom_rule_policy() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutConfigRule",
        json!({ "ConfigRule": { "ConfigRuleName": "policy-rule", "Source": { "Owner": "CUSTOM_POLICY", "CustomPolicyDetails": { "PolicyRuntime": "guard-2.x.x", "PolicyText": "rule x { true }" } } } }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "GetCustomRulePolicy",
        json!({ "ConfigRuleName": "policy-rule" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Compliance ───────────────────────────────────────────────────────────────

#[test_action("config", "DescribeComplianceByConfigRule", checksum = "7d15fa29")]
#[tokio::test]
async fn describe_compliance_by_config_rule() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(&server, "DescribeComplianceByConfigRule", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeComplianceByResource", checksum = "5ef4a419")]
#[tokio::test]
async fn describe_compliance_by_resource() {
    let server = TestServer::start().await;
    let (s, b) = cfg(&server, "DescribeComplianceByResource", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetComplianceDetailsByConfigRule", checksum = "5baa2cd0")]
#[tokio::test]
async fn get_compliance_details_by_config_rule() {
    let server = TestServer::start().await;
    make_rule(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "GetComplianceDetailsByConfigRule",
        json!({ "ConfigRuleName": "rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetComplianceDetailsByResource", checksum = "d15f45c5")]
#[tokio::test]
async fn get_compliance_details_by_resource() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "GetComplianceDetailsByResource",
        json!({ "ResourceType": "AWS::S3::Bucket", "ResourceId": "b1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetComplianceSummaryByConfigRule", checksum = "3cdc436c")]
#[tokio::test]
async fn get_compliance_summary_by_config_rule() {
    let server = TestServer::start().await;
    let (s, b) = cfg(&server, "GetComplianceSummaryByConfigRule", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetComplianceSummaryByResourceType", checksum = "d246ea0f")]
#[tokio::test]
async fn get_compliance_summary_by_resource_type() {
    let server = TestServer::start().await;
    let (s, b) = cfg(&server, "GetComplianceSummaryByResourceType", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

// ── Remediation ──────────────────────────────────────────────────────────────

async fn put_remediation(server: &TestServer, rule: &str) {
    make_rule(server, rule).await;
    let (s, b) = cfg(
        server,
        "PutRemediationConfigurations",
        json!({ "RemediationConfigurations": [{ "ConfigRuleName": rule, "TargetType": "SSM_DOCUMENT", "TargetId": "AWS-PublishSNSNotification" }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutRemediationConfigurations", checksum = "3ebb19e5")]
#[tokio::test]
async fn put_remediation_configurations() {
    let server = TestServer::start().await;
    put_remediation(&server, "rule1").await;
}

#[test_action("config", "DescribeRemediationConfigurations", checksum = "e853b339")]
#[tokio::test]
async fn describe_remediation_configurations() {
    let server = TestServer::start().await;
    put_remediation(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "DescribeRemediationConfigurations",
        json!({ "ConfigRuleNames": ["rule1"] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteRemediationConfiguration", checksum = "4c0b5765")]
#[tokio::test]
async fn delete_remediation_configuration() {
    let server = TestServer::start().await;
    put_remediation(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "DeleteRemediationConfiguration",
        json!({ "ConfigRuleName": "rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutRemediationExceptions", checksum = "e92807e2")]
#[tokio::test]
async fn put_remediation_exceptions() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "PutRemediationExceptions",
        json!({ "ConfigRuleName": "rule1", "ResourceKeys": [{ "ResourceType": "AWS::S3::Bucket", "ResourceId": "b1" }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeRemediationExceptions", checksum = "033f4b79")]
#[tokio::test]
async fn describe_remediation_exceptions() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "DescribeRemediationExceptions",
        json!({ "ConfigRuleName": "rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteRemediationExceptions", checksum = "70748cc2")]
#[tokio::test]
async fn delete_remediation_exceptions() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "DeleteRemediationExceptions",
        json!({ "ConfigRuleName": "rule1", "ResourceKeys": [{ "ResourceType": "AWS::S3::Bucket", "ResourceId": "b1" }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "StartRemediationExecution", checksum = "41229694")]
#[tokio::test]
async fn start_remediation_execution() {
    let server = TestServer::start().await;
    put_remediation(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "StartRemediationExecution",
        json!({ "ConfigRuleName": "rule1", "ResourceKeys": [{ "ResourceType": "AWS::S3::Bucket", "ResourceId": "b1" }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeRemediationExecutionStatus", checksum = "67ccbc43")]
#[tokio::test]
async fn describe_remediation_execution_status() {
    let server = TestServer::start().await;
    put_remediation(&server, "rule1").await;
    let (s, b) = cfg(
        &server,
        "DescribeRemediationExecutionStatus",
        json!({ "ConfigRuleName": "rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Conformance packs ────────────────────────────────────────────────────────

#[test_action("config", "PutConformancePack", checksum = "54914d7d")]
#[tokio::test]
async fn put_conformance_pack() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
}

#[test_action("config", "DescribeConformancePacks", checksum = "4defac72")]
#[tokio::test]
async fn describe_conformance_packs() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(&server, "DescribeConformancePacks", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteConformancePack", checksum = "8446a9db")]
#[tokio::test]
async fn delete_conformance_pack() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(
        &server,
        "DeleteConformancePack",
        json!({ "ConformancePackName": "pack1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeConformancePackStatus", checksum = "4a6d5543")]
#[tokio::test]
async fn describe_conformance_pack_status() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(&server, "DescribeConformancePackStatus", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeConformancePackCompliance", checksum = "9f007ecf")]
#[tokio::test]
async fn describe_conformance_pack_compliance() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(
        &server,
        "DescribeConformancePackCompliance",
        json!({ "ConformancePackName": "pack1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetConformancePackComplianceSummary", checksum = "dcaec492")]
#[tokio::test]
async fn get_conformance_pack_compliance_summary() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(
        &server,
        "GetConformancePackComplianceSummary",
        json!({ "ConformancePackNames": ["pack1"] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetConformancePackComplianceDetails", checksum = "ef0b52e1")]
#[tokio::test]
async fn get_conformance_pack_compliance_details() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(
        &server,
        "GetConformancePackComplianceDetails",
        json!({ "ConformancePackName": "pack1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "ListConformancePackComplianceScores", checksum = "ede62446")]
#[tokio::test]
async fn list_conformance_pack_compliance_scores() {
    let server = TestServer::start().await;
    make_pack(&server, "pack1").await;
    let (s, b) = cfg(&server, "ListConformancePackComplianceScores", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

// ── Organization rules / packs ───────────────────────────────────────────────

async fn make_org_rule(server: &TestServer, name: &str) {
    let (s, b) = cfg(
        server,
        "PutOrganizationConfigRule",
        json!({ "OrganizationConfigRuleName": name, "OrganizationManagedRuleMetadata": { "RuleIdentifier": "S3_BUCKET_VERSIONING_ENABLED" } }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

async fn make_org_pack(server: &TestServer, name: &str) {
    let (s, b) = cfg(
        server,
        "PutOrganizationConformancePack",
        json!({ "OrganizationConformancePackName": name, "TemplateBody": "Resources: {}" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutOrganizationConfigRule", checksum = "40c62465")]
#[tokio::test]
async fn put_organization_config_rule() {
    let server = TestServer::start().await;
    make_org_rule(&server, "org-rule1").await;
}

#[test_action("config", "DescribeOrganizationConfigRules", checksum = "8e6c69c2")]
#[tokio::test]
async fn describe_organization_config_rules() {
    let server = TestServer::start().await;
    make_org_rule(&server, "org-rule1").await;
    let (s, b) = cfg(&server, "DescribeOrganizationConfigRules", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteOrganizationConfigRule", checksum = "18ac9f85")]
#[tokio::test]
async fn delete_organization_config_rule() {
    let server = TestServer::start().await;
    make_org_rule(&server, "org-rule1").await;
    let (s, b) = cfg(
        &server,
        "DeleteOrganizationConfigRule",
        json!({ "OrganizationConfigRuleName": "org-rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "DescribeOrganizationConfigRuleStatuses",
    checksum = "088b7ccd"
)]
#[tokio::test]
async fn describe_organization_config_rule_statuses() {
    let server = TestServer::start().await;
    make_org_rule(&server, "org-rule1").await;
    let (s, b) = cfg(&server, "DescribeOrganizationConfigRuleStatuses", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "GetOrganizationConfigRuleDetailedStatus",
    checksum = "8d7bc0f1"
)]
#[tokio::test]
async fn get_organization_config_rule_detailed_status() {
    let server = TestServer::start().await;
    make_org_rule(&server, "org-rule1").await;
    let (s, b) = cfg(
        &server,
        "GetOrganizationConfigRuleDetailedStatus",
        json!({ "OrganizationConfigRuleName": "org-rule1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetOrganizationCustomRulePolicy", checksum = "e84800b8")]
#[tokio::test]
async fn get_organization_custom_rule_policy() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutOrganizationConfigRule",
        json!({ "OrganizationConfigRuleName": "org-policy", "OrganizationCustomPolicyRuleMetadata": { "PolicyRuntime": "guard-2.x.x", "PolicyText": "rule x { true }", "OrganizationConfigRuleTriggerTypes": ["ConfigurationItemChangeNotification"] } }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "GetOrganizationCustomRulePolicy",
        json!({ "OrganizationConfigRuleName": "org-policy" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutOrganizationConformancePack", checksum = "582bc320")]
#[tokio::test]
async fn put_organization_conformance_pack() {
    let server = TestServer::start().await;
    make_org_pack(&server, "org-pack1").await;
}

#[test_action(
    "config",
    "DescribeOrganizationConformancePacks",
    checksum = "fea0f932"
)]
#[tokio::test]
async fn describe_organization_conformance_packs() {
    let server = TestServer::start().await;
    make_org_pack(&server, "org-pack1").await;
    let (s, b) = cfg(&server, "DescribeOrganizationConformancePacks", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteOrganizationConformancePack", checksum = "83e1fa93")]
#[tokio::test]
async fn delete_organization_conformance_pack() {
    let server = TestServer::start().await;
    make_org_pack(&server, "org-pack1").await;
    let (s, b) = cfg(
        &server,
        "DeleteOrganizationConformancePack",
        json!({ "OrganizationConformancePackName": "org-pack1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "DescribeOrganizationConformancePackStatuses",
    checksum = "b5b7eec8"
)]
#[tokio::test]
async fn describe_organization_conformance_pack_statuses() {
    let server = TestServer::start().await;
    make_org_pack(&server, "org-pack1").await;
    let (s, b) = cfg(
        &server,
        "DescribeOrganizationConformancePackStatuses",
        json!({}),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "GetOrganizationConformancePackDetailedStatus",
    checksum = "f5c27b12"
)]
#[tokio::test]
async fn get_organization_conformance_pack_detailed_status() {
    let server = TestServer::start().await;
    make_org_pack(&server, "org-pack1").await;
    let (s, b) = cfg(
        &server,
        "GetOrganizationConformancePackDetailedStatus",
        json!({ "OrganizationConformancePackName": "org-pack1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Aggregators ──────────────────────────────────────────────────────────────

#[test_action("config", "PutConfigurationAggregator", checksum = "f67a0277")]
#[tokio::test]
async fn put_configuration_aggregator() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
}

#[test_action("config", "DescribeConfigurationAggregators", checksum = "e0a38e4b")]
#[tokio::test]
async fn describe_configuration_aggregators() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(&server, "DescribeConfigurationAggregators", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteConfigurationAggregator", checksum = "cb5a4caf")]
#[tokio::test]
async fn delete_configuration_aggregator() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "DeleteConfigurationAggregator",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "DescribeConfigurationAggregatorSourcesStatus",
    checksum = "3d46d876"
)]
#[tokio::test]
async fn describe_configuration_aggregator_sources_status() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "DescribeConfigurationAggregatorSourcesStatus",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutAggregationAuthorization", checksum = "e1c4df0c")]
#[tokio::test]
async fn put_aggregation_authorization() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "PutAggregationAuthorization",
        json!({ "AuthorizedAccountId": "210987654321", "AuthorizedAwsRegion": "us-west-2" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeAggregationAuthorizations", checksum = "1d6fea31")]
#[tokio::test]
async fn describe_aggregation_authorizations() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutAggregationAuthorization",
        json!({ "AuthorizedAccountId": "210987654321", "AuthorizedAwsRegion": "us-west-2" }),
    )
    .await;
    let (s, b) = cfg(&server, "DescribeAggregationAuthorizations", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteAggregationAuthorization", checksum = "3d77b6f3")]
#[tokio::test]
async fn delete_aggregation_authorization() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutAggregationAuthorization",
        json!({ "AuthorizedAccountId": "210987654321", "AuthorizedAwsRegion": "us-west-2" }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "DeleteAggregationAuthorization",
        json!({ "AuthorizedAccountId": "210987654321", "AuthorizedAwsRegion": "us-west-2" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeletePendingAggregationRequest", checksum = "00bdbe0a")]
#[tokio::test]
async fn delete_pending_aggregation_request() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "DeletePendingAggregationRequest",
        json!({ "RequesterAccountId": "210987654321", "RequesterAwsRegion": "us-west-2" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribePendingAggregationRequests", checksum = "9b3b8bfa")]
#[tokio::test]
async fn describe_pending_aggregation_requests() {
    let server = TestServer::start().await;
    let (s, b) = cfg(&server, "DescribePendingAggregationRequests", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "BatchGetAggregateResourceConfig", checksum = "5f5ba780")]
#[tokio::test]
async fn batch_get_aggregate_resource_config() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    put_item(&server).await;
    let (s, b) = cfg(
        &server,
        "BatchGetAggregateResourceConfig",
        json!({ "ConfigurationAggregatorName": "agg1", "ResourceIdentifiers": [{ "SourceAccountId": "123456789012", "SourceRegion": "us-east-1", "ResourceType": "AWS::EC2::CustomerGateway", "ResourceId": "cgw-1234" }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "GetAggregateResourceConfig", checksum = "8be3126f")]
#[tokio::test]
async fn get_aggregate_resource_config() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    put_item(&server).await;
    let (s, b) = cfg(
        &server,
        "GetAggregateResourceConfig",
        json!({ "ConfigurationAggregatorName": "agg1", "ResourceIdentifier": { "SourceAccountId": "123456789012", "SourceRegion": "us-east-1", "ResourceType": "AWS::EC2::CustomerGateway", "ResourceId": "cgw-1234" } }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "ListAggregateDiscoveredResources", checksum = "921c0832")]
#[tokio::test]
async fn list_aggregate_discovered_resources() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "ListAggregateDiscoveredResources",
        json!({ "ConfigurationAggregatorName": "agg1", "ResourceType": "AWS::S3::Bucket" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "GetAggregateDiscoveredResourceCounts",
    checksum = "20b1cd69"
)]
#[tokio::test]
async fn get_aggregate_discovered_resource_counts() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "GetAggregateDiscoveredResourceCounts",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "DescribeAggregateComplianceByConfigRules",
    checksum = "24e153f0"
)]
#[tokio::test]
async fn describe_aggregate_compliance_by_config_rules() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "DescribeAggregateComplianceByConfigRules",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "DescribeAggregateComplianceByConformancePacks",
    checksum = "462b15a6"
)]
#[tokio::test]
async fn describe_aggregate_compliance_by_conformance_packs() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "DescribeAggregateComplianceByConformancePacks",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "GetAggregateComplianceDetailsByConfigRule",
    checksum = "ca4a6809"
)]
#[tokio::test]
async fn get_aggregate_compliance_details_by_config_rule() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "GetAggregateComplianceDetailsByConfigRule",
        json!({ "ConfigurationAggregatorName": "agg1", "ConfigRuleName": "rule1", "AccountId": "123456789012", "AwsRegion": "us-east-1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "GetAggregateConfigRuleComplianceSummary",
    checksum = "2e73ede4"
)]
#[tokio::test]
async fn get_aggregate_config_rule_compliance_summary() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "GetAggregateConfigRuleComplianceSummary",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action(
    "config",
    "GetAggregateConformancePackComplianceSummary",
    checksum = "6a5356f0"
)]
#[tokio::test]
async fn get_aggregate_conformance_pack_compliance_summary() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "GetAggregateConformancePackComplianceSummary",
        json!({ "ConfigurationAggregatorName": "agg1" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "SelectAggregateResourceConfig", checksum = "9939f7ec")]
#[tokio::test]
async fn select_aggregate_resource_config() {
    let server = TestServer::start().await;
    make_aggregator(&server, "agg1").await;
    let (s, b) = cfg(
        &server,
        "SelectAggregateResourceConfig",
        json!({ "ConfigurationAggregatorName": "agg1", "Expression": "SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Retention ────────────────────────────────────────────────────────────────

#[test_action("config", "PutRetentionConfiguration", checksum = "4b137c6e")]
#[tokio::test]
async fn put_retention_configuration() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "PutRetentionConfiguration",
        json!({ "RetentionPeriodInDays": 2557 }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DescribeRetentionConfigurations", checksum = "7a867c05")]
#[tokio::test]
async fn describe_retention_configurations() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutRetentionConfiguration",
        json!({ "RetentionPeriodInDays": 2557 }),
    )
    .await;
    let (s, b) = cfg(&server, "DescribeRetentionConfigurations", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "DeleteRetentionConfiguration", checksum = "b2dcfb53")]
#[tokio::test]
async fn delete_retention_configuration() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "PutRetentionConfiguration",
        json!({ "RetentionPeriodInDays": 2557 }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "DeleteRetentionConfiguration",
        json!({ "RetentionConfigurationName": "default" }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

// ── Stored queries ───────────────────────────────────────────────────────────

async fn put_query(server: &TestServer, name: &str) {
    let (s, b) = cfg(
        server,
        "PutStoredQuery",
        json!({ "StoredQuery": { "QueryName": name, "Expression": "SELECT resourceId" } }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "PutStoredQuery", checksum = "71f62678")]
#[tokio::test]
async fn put_stored_query() {
    let server = TestServer::start().await;
    put_query(&server, "q1").await;
}

#[test_action("config", "GetStoredQuery", checksum = "bcf7fed3")]
#[tokio::test]
async fn get_stored_query() {
    let server = TestServer::start().await;
    put_query(&server, "q1").await;
    let (s, b) = cfg(&server, "GetStoredQuery", json!({ "QueryName": "q1" })).await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["StoredQuery"]["QueryName"], "q1");
}

#[test_action("config", "DeleteStoredQuery", checksum = "2af2cbc1")]
#[tokio::test]
async fn delete_stored_query() {
    let server = TestServer::start().await;
    put_query(&server, "q1").await;
    let (s, b) = cfg(&server, "DeleteStoredQuery", json!({ "QueryName": "q1" })).await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "ListStoredQueries", checksum = "0f9a22b7")]
#[tokio::test]
async fn list_stored_queries() {
    let server = TestServer::start().await;
    put_query(&server, "q1").await;
    let (s, b) = cfg(&server, "ListStoredQueries", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

// ── Resource evaluations ─────────────────────────────────────────────────────

#[test_action("config", "StartResourceEvaluation", checksum = "ca71be0a")]
#[tokio::test]
async fn start_resource_evaluation() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "StartResourceEvaluation",
        json!({
            "EvaluationMode": "PROACTIVE",
            "ResourceDetails": { "ResourceId": "my-bucket", "ResourceType": "AWS::S3::Bucket", "ResourceConfiguration": "{}" },
        }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert!(b["ResourceEvaluationId"].as_str().is_some());
}

#[test_action("config", "GetResourceEvaluationSummary", checksum = "7db22504")]
#[tokio::test]
async fn get_resource_evaluation_summary() {
    let server = TestServer::start().await;
    let (_, created) = cfg(
        &server,
        "StartResourceEvaluation",
        json!({
            "EvaluationMode": "PROACTIVE",
            "ResourceDetails": { "ResourceId": "my-bucket", "ResourceType": "AWS::S3::Bucket", "ResourceConfiguration": "{}" },
        }),
    )
    .await;
    let id = created["ResourceEvaluationId"].as_str().unwrap();
    let (s, b) = cfg(
        &server,
        "GetResourceEvaluationSummary",
        json!({ "ResourceEvaluationId": id }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "ListResourceEvaluations", checksum = "0715f26b")]
#[tokio::test]
async fn list_resource_evaluations() {
    let server = TestServer::start().await;
    let (s, b) = cfg(&server, "ListResourceEvaluations", json!({})).await;
    assert_eq!(s, 200, "{b}");
}

// ── Select ───────────────────────────────────────────────────────────────────

#[test_action("config", "SelectResourceConfig", checksum = "f47e8d79")]
#[tokio::test]
async fn select_resource_config() {
    let server = TestServer::start().await;
    let (s, b) = cfg(&server, "SelectResourceConfig", json!({ "Expression": "SELECT resourceId, resourceType WHERE resourceType = 'AWS::S3::Bucket'" })).await;
    assert_eq!(s, 200, "{b}");
    assert!(b["Results"].is_array());
}

// ── Tags ─────────────────────────────────────────────────────────────────────

const RULE_ARN: &str = "arn:aws:config:us-east-1:123456789012:config-rule/config-rule-abc123";

#[test_action("config", "TagResource", checksum = "0187a40b")]
#[tokio::test]
async fn tag_resource() {
    let server = TestServer::start().await;
    let (s, b) = cfg(
        &server,
        "TagResource",
        json!({ "ResourceArn": RULE_ARN, "Tags": [{ "Key": "env", "Value": "test" }] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}

#[test_action("config", "ListTagsForResource", checksum = "546787a7")]
#[tokio::test]
async fn list_tags_for_resource() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "TagResource",
        json!({ "ResourceArn": RULE_ARN, "Tags": [{ "Key": "env", "Value": "test" }] }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "ListTagsForResource",
        json!({ "ResourceArn": RULE_ARN }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["Tags"][0]["Key"], "env");
}

#[test_action("config", "UntagResource", checksum = "9cd407bf")]
#[tokio::test]
async fn untag_resource() {
    let server = TestServer::start().await;
    cfg(
        &server,
        "TagResource",
        json!({ "ResourceArn": RULE_ARN, "Tags": [{ "Key": "env", "Value": "test" }] }),
    )
    .await;
    let (s, b) = cfg(
        &server,
        "UntagResource",
        json!({ "ResourceArn": RULE_ARN, "TagKeys": ["env"] }),
    )
    .await;
    assert_eq!(s, 200, "{b}");
}
