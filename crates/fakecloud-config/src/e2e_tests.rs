//! End-to-end handler tests for AWS Config, driving [`ConfigService::handle`]
//! with hand-built awsJson1_1 `AwsRequest`s. These prove real behaviour rather
//! than response shape: multi-account evaluation isolation, real proactive
//! resource evaluation, pagination round-trips, per-resource-type compliance
//! summaries, and delivery-channel S3 bucket validation.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, ResponseBody};
use fakecloud_s3::{S3Bucket, SharedS3State};

use crate::state::{ConfigAccounts, SharedConfigState};
use crate::validate::CrossServiceStates;
use crate::ConfigService;

fn service() -> ConfigService {
    let state: SharedConfigState = Arc::new(RwLock::new(ConfigAccounts::new()));
    ConfigService::new(state)
}

fn service_with_s3(bucket_names: &[&str]) -> ConfigService {
    let s3: SharedS3State = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    {
        let mut guard = s3.write();
        let acc = guard.get_or_create("000000000000");
        for name in bucket_names {
            acc.buckets.insert(
                name.to_string(),
                S3Bucket::new(name, "us-east-1", "000000000000"),
            );
        }
    }
    let cross = CrossServiceStates {
        s3: Some(s3),
        ..Default::default()
    };
    let state: SharedConfigState = Arc::new(RwLock::new(ConfigAccounts::new()));
    ConfigService::new(state).with_cross_service(cross)
}

fn req(action: &str, account: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "config".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: account.to_string(),
        request_id: "test".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

async fn call(svc: &ConfigService, action: &str, account: &str, body: Value) -> Value {
    let resp = svc
        .handle(req(action, account, body))
        .await
        .expect("handler returned an error");
    body_of(&resp)
}

async fn call_err(svc: &ConfigService, action: &str, account: &str, body: Value) -> (u16, String) {
    match svc.handle(req(action, account, body)).await {
        Ok(_) => panic!("expected an error, got success"),
        Err(e) => (e.status().as_u16(), e.code().to_string()),
    }
}

fn body_of(resp: &AwsResponse) -> Value {
    match &resp.body {
        ResponseBody::Bytes(b) if !b.is_empty() => serde_json::from_slice(b).unwrap(),
        _ => Value::Null,
    }
}

fn managed_rule_body(name: &str, source_identifier: &str) -> Value {
    json!({
        "ConfigRule": {
            "ConfigRuleName": name,
            "Source": { "Owner": "AWS", "SourceIdentifier": source_identifier },
        }
    })
}

/// A second account's newly created rule must actually evaluate. A single
/// global evaluation gate would let the first account's evaluation suppress the
/// second account's forever (stale / never-evaluated).
#[tokio::test]
async fn multi_account_evaluation_is_isolated() {
    let svc = service();
    let acct_a = "111111111111";
    let acct_b = "222222222222";

    // Create the same managed rule in both accounts BEFORE any read, so the
    // state version does not change between evaluating A and reading B — the
    // exact condition under which a global gate wrongly skips B.
    call(
        &svc,
        "PutConfigRule",
        acct_a,
        managed_rule_body("r", "S3_BUCKET_VERSIONING_ENABLED"),
    )
    .await;
    call(
        &svc,
        "PutConfigRule",
        acct_b,
        managed_rule_body("r", "S3_BUCKET_VERSIONING_ENABLED"),
    )
    .await;

    // Evaluate account A first.
    call(
        &svc,
        "DescribeConfigRuleEvaluationStatus",
        acct_a,
        json!({}),
    )
    .await;

    // Now account B must also evaluate (its rule gets a successful-evaluation
    // time), not be suppressed by A's evaluation.
    let b = call(
        &svc,
        "DescribeConfigRuleEvaluationStatus",
        acct_b,
        json!({}),
    )
    .await;
    let status = &b["ConfigRulesEvaluationStatus"][0];
    assert!(
        !status["LastSuccessfulEvaluationTime"].is_null(),
        "account B's rule must have been evaluated: {b}"
    );
    assert_eq!(status["FirstEvaluationStarted"], json!(true));
}

/// StartResourceEvaluation must evaluate the supplied configuration against the
/// applicable rules and report the true compliance, not a canned COMPLIANT.
#[tokio::test]
async fn proactive_resource_evaluation_reports_real_compliance() {
    let svc = service();
    call(
        &svc,
        "PutConfigRule",
        "000000000000",
        managed_rule_body("versioning", "S3_BUCKET_VERSIONING_ENABLED"),
    )
    .await;

    // A bucket with versioning Off is NON_COMPLIANT.
    let start = call(
        &svc,
        "StartResourceEvaluation",
        "000000000000",
        json!({
            "EvaluationMode": "DETECTIVE",
            "ResourceDetails": {
                "ResourceId": "bad-bucket",
                "ResourceType": "AWS::S3::Bucket",
                "ResourceConfiguration": "{\"bucketVersioningConfiguration\":{\"status\":\"Off\"}}",
            },
        }),
    )
    .await;
    let id = start["ResourceEvaluationId"].as_str().unwrap().to_string();
    let summary = call(
        &svc,
        "GetResourceEvaluationSummary",
        "000000000000",
        json!({ "ResourceEvaluationId": id }),
    )
    .await;
    assert_eq!(summary["Compliance"], json!("NON_COMPLIANT"));

    // A bucket with versioning Enabled is COMPLIANT.
    let start = call(
        &svc,
        "StartResourceEvaluation",
        "000000000000",
        json!({
            "EvaluationMode": "DETECTIVE",
            "ResourceDetails": {
                "ResourceId": "good-bucket",
                "ResourceType": "AWS::S3::Bucket",
                "ResourceConfiguration": "{\"bucketVersioningConfiguration\":{\"status\":\"Enabled\"}}",
            },
        }),
    )
    .await;
    let id = start["ResourceEvaluationId"].as_str().unwrap().to_string();
    let summary = call(
        &svc,
        "GetResourceEvaluationSummary",
        "000000000000",
        json!({ "ResourceEvaluationId": id }),
    )
    .await;
    assert_eq!(summary["Compliance"], json!("COMPLIANT"));
}

/// No applicable rule -> honest INSUFFICIENT_DATA, never a fabricated pass.
#[tokio::test]
async fn proactive_resource_evaluation_without_rules_is_insufficient_data() {
    let svc = service();
    let start = call(
        &svc,
        "StartResourceEvaluation",
        "000000000000",
        json!({
            "EvaluationMode": "DETECTIVE",
            "ResourceDetails": {
                "ResourceId": "b",
                "ResourceType": "AWS::S3::Bucket",
                "ResourceConfiguration": "{}",
            },
        }),
    )
    .await;
    let id = start["ResourceEvaluationId"].as_str().unwrap().to_string();
    let summary = call(
        &svc,
        "GetResourceEvaluationSummary",
        "000000000000",
        json!({ "ResourceEvaluationId": id }),
    )
    .await;
    assert_eq!(summary["Compliance"], json!("INSUFFICIENT_DATA"));
}

/// DescribeConfigRules must honor Limit/NextToken and round-trip the full set.
#[tokio::test]
async fn describe_config_rules_paginates() {
    let svc = service();
    for i in 0..3 {
        call(
            &svc,
            "PutConfigRule",
            "000000000000",
            managed_rule_body(&format!("rule-{i}"), "S3_BUCKET_VERSIONING_ENABLED"),
        )
        .await;
    }

    let page1 = call(
        &svc,
        "DescribeConfigRules",
        "000000000000",
        json!({ "Limit": 2 }),
    )
    .await;
    assert_eq!(page1["ConfigRules"].as_array().unwrap().len(), 2);
    let token = page1["NextToken"].as_str().expect("token on first page");

    let page2 = call(
        &svc,
        "DescribeConfigRules",
        "000000000000",
        json!({ "Limit": 2, "NextToken": token }),
    )
    .await;
    assert_eq!(page2["ConfigRules"].as_array().unwrap().len(), 1);
    assert!(page2["NextToken"].is_null(), "last page has no token");

    // Collected names cover the full set with no duplicates.
    let mut names: Vec<String> = page1["ConfigRules"]
        .as_array()
        .unwrap()
        .iter()
        .chain(page2["ConfigRules"].as_array().unwrap())
        .map(|r| r["ConfigRuleName"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["rule-0", "rule-1", "rule-2"]);
}

/// GetComplianceSummaryByResourceType returns one summary per requested type,
/// each with that type's own counts (not one bucket summed across all types).
#[tokio::test]
async fn compliance_summary_by_resource_type_is_per_type() {
    let svc = service();
    // Seed evaluations for two different resource types via PutEvaluations
    // (the token prefix before '#' is the rule name; using a name with no
    // PutConfigRule means the managed re-evaluation never clobbers it).
    call(
        &svc,
        "PutEvaluations",
        "000000000000",
        json!({
            "ResultToken": "seed#tok",
            "Evaluations": [
                { "ComplianceResourceType": "AWS::S3::Bucket", "ComplianceResourceId": "b1", "ComplianceType": "COMPLIANT" },
                { "ComplianceResourceType": "AWS::EC2::Instance", "ComplianceResourceId": "i1", "ComplianceType": "NON_COMPLIANT" },
            ],
        }),
    )
    .await;

    let out = call(
        &svc,
        "GetComplianceSummaryByResourceType",
        "000000000000",
        json!({ "ResourceTypes": ["AWS::S3::Bucket", "AWS::EC2::Instance"] }),
    )
    .await;
    let summaries = out["ComplianceSummariesByResourceType"].as_array().unwrap();
    assert_eq!(summaries.len(), 2);

    let by_type: HashMap<&str, &Value> = summaries
        .iter()
        .map(|s| (s["ResourceType"].as_str().unwrap(), &s["ComplianceSummary"]))
        .collect();
    let s3 = by_type["AWS::S3::Bucket"];
    assert_eq!(s3["CompliantResourceCount"]["CappedCount"], json!(1));
    assert_eq!(s3["NonCompliantResourceCount"]["CappedCount"], json!(0));
    let ec2 = by_type["AWS::EC2::Instance"];
    assert_eq!(ec2["CompliantResourceCount"]["CappedCount"], json!(0));
    assert_eq!(ec2["NonCompliantResourceCount"]["CappedCount"], json!(1));
}

/// PutDeliveryChannel rejects a delivery channel that points at a non-existent
/// S3 bucket when S3 is wired.
#[tokio::test]
async fn put_delivery_channel_validates_bucket_exists() {
    let svc = service_with_s3(&["real-bucket"]);

    // Existing bucket succeeds.
    call(
        &svc,
        "PutDeliveryChannel",
        "000000000000",
        json!({ "DeliveryChannel": { "name": "default", "s3BucketName": "real-bucket" } }),
    )
    .await;

    // Missing bucket is rejected.
    let (status, code) = call_err(
        &svc,
        "PutDeliveryChannel",
        "000000000000",
        json!({ "DeliveryChannel": { "name": "default", "s3BucketName": "ghost-bucket" } }),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code, "NoSuchBucketException");
}

/// Without S3 wired, bucket validation is skipped (graceful degradation).
#[tokio::test]
async fn put_delivery_channel_skips_validation_when_s3_absent() {
    let svc = service();
    call(
        &svc,
        "PutDeliveryChannel",
        "000000000000",
        json!({ "DeliveryChannel": { "name": "default", "s3BucketName": "any-bucket" } }),
    )
    .await;
}

/// StartRemediationExecution records real executions surfaced by
/// DescribeRemediationExecutionStatus, honestly QUEUED rather than a fabricated
/// SUCCEEDED for arbitrary keys.
#[tokio::test]
async fn remediation_execution_is_tracked_honestly() {
    let svc = service();
    call(
        &svc,
        "PutConfigRule",
        "000000000000",
        managed_rule_body("r", "S3_BUCKET_VERSIONING_ENABLED"),
    )
    .await;
    call(
        &svc,
        "PutRemediationConfigurations",
        "000000000000",
        json!({
            "RemediationConfigurations": [
                { "ConfigRuleName": "r", "TargetType": "SSM_DOCUMENT", "TargetId": "AWS-EnableS3BucketVersioning" }
            ]
        }),
    )
    .await;

    // No executions before any StartRemediationExecution.
    let before = call(
        &svc,
        "DescribeRemediationExecutionStatus",
        "000000000000",
        json!({ "ConfigRuleName": "r" }),
    )
    .await;
    assert_eq!(
        before["RemediationExecutionStatuses"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    call(
        &svc,
        "StartRemediationExecution",
        "000000000000",
        json!({
            "ConfigRuleName": "r",
            "ResourceKeys": [ { "resourceType": "AWS::S3::Bucket", "resourceId": "b1" } ]
        }),
    )
    .await;

    let after = call(
        &svc,
        "DescribeRemediationExecutionStatus",
        "000000000000",
        json!({ "ConfigRuleName": "r" }),
    )
    .await;
    let list = after["RemediationExecutionStatuses"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["State"], json!("QUEUED"));
    assert_eq!(list[0]["ResourceKey"]["resourceId"], json!("b1"));
}

/// GetResourceConfigHistory honors chronologicalOrder and time-window filters.
#[tokio::test]
async fn resource_config_history_honors_order_and_time_window() {
    let svc = service();
    // Record the same resource twice to build a two-entry history.
    for _ in 0..2 {
        call(
            &svc,
            "PutResourceConfig",
            "000000000000",
            json!({
                "ResourceType": "AWS::CloudFormation::Stack",
                "ResourceId": "res-1",
                "Configuration": "{}",
                "SchemaVersionId": "1.0",
            }),
        )
        .await;
    }

    // Default order is Reverse (newest first); Forward flips it.
    let reverse = call(
        &svc,
        "GetResourceConfigHistory",
        "000000000000",
        json!({ "resourceType": "AWS::CloudFormation::Stack", "resourceId": "res-1" }),
    )
    .await;
    let forward = call(
        &svc,
        "GetResourceConfigHistory",
        "000000000000",
        json!({ "resourceType": "AWS::CloudFormation::Stack", "resourceId": "res-1", "chronologicalOrder": "Forward" }),
    )
    .await;
    let rev_items = reverse["configurationItems"].as_array().unwrap();
    let fwd_items = forward["configurationItems"].as_array().unwrap();
    assert_eq!(rev_items.len(), fwd_items.len());
    assert!(rev_items.len() >= 2);
    // Reverse should be the exact reverse ordering of Forward.
    let rev_states: Vec<&Value> = rev_items
        .iter()
        .map(|i| &i["configurationStateId"])
        .collect();
    let mut fwd_states: Vec<&Value> = fwd_items
        .iter()
        .map(|i| &i["configurationStateId"])
        .collect();
    fwd_states.reverse();
    assert_eq!(rev_states, fwd_states);

    // A laterTime in the distant past excludes everything.
    let none = call(
        &svc,
        "GetResourceConfigHistory",
        "000000000000",
        json!({ "resourceType": "AWS::CloudFormation::Stack", "resourceId": "res-1", "laterTime": 1.0 }),
    )
    .await;
    assert_eq!(none["configurationItems"].as_array().unwrap().len(), 0);
}
