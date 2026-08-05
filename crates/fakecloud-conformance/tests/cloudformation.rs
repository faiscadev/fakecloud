mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const SIMPLE_TEMPLATE: &str = r#"{
    "Resources": {
        "MyQueue": {
            "Type": "AWS::SQS::Queue",
            "Properties": {
                "QueueName": "cf-conf-queue"
            }
        }
    }
}"#;

// ---------------------------------------------------------------------------
// Stack lifecycle
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "CreateStack", checksum = "305fd6f4")]
#[test_action("cloudformation", "DescribeStacks", checksum = "1cb6a592")]
#[test_action("cloudformation", "DeleteStack", checksum = "83ff08df")]
#[tokio::test]
async fn cloudformation_create_describe_delete_stack() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    let result = client
        .create_stack()
        .stack_name("conf-stack")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();
    assert!(result.stack_id().is_some());

    let desc = client
        .describe_stacks()
        .stack_name("conf-stack")
        .send()
        .await
        .unwrap();
    let stacks = desc.stacks();
    assert_eq!(stacks.len(), 1);
    assert_eq!(stacks[0].stack_name(), Some("conf-stack"));
    assert_eq!(
        stacks[0].stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE")
    );

    client
        .delete_stack()
        .stack_name("conf-stack")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudformation", "ListStacks", checksum = "0462876a")]
#[tokio::test]
async fn cloudformation_list_stacks() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    client
        .create_stack()
        .stack_name("list-stack-a")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();

    let template2 = r#"{
        "Resources": {
            "Q2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-conf-queue-2" }
            }
        }
    }"#;

    client
        .create_stack()
        .stack_name("list-stack-b")
        .template_body(template2)
        .send()
        .await
        .unwrap();

    let resp = client.list_stacks().send().await.unwrap();
    assert!(resp.stack_summaries().len() >= 2);
}

// ---------------------------------------------------------------------------
// Stack resources
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "ListStackResources", checksum = "471df8aa")]
#[tokio::test]
async fn cloudformation_list_stack_resources() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "Queue1": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-res-q1" }
            },
            "Queue2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-res-q2" }
            }
        }
    }"#;

    client
        .create_stack()
        .stack_name("resources-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = client
        .list_stack_resources()
        .stack_name("resources-stack")
        .send()
        .await
        .unwrap();

    let summaries = result.stack_resource_summaries();
    assert_eq!(summaries.len(), 2);

    let logical_ids: Vec<&str> = summaries
        .iter()
        .filter_map(|r| r.logical_resource_id())
        .collect();
    assert!(logical_ids.contains(&"Queue1"));
    assert!(logical_ids.contains(&"Queue2"));
}

#[test_action("cloudformation", "DescribeStackResources", checksum = "74d268a4")]
#[tokio::test]
async fn cloudformation_describe_stack_resources() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    client
        .create_stack()
        .stack_name("dsr-stack")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();

    let result = client
        .describe_stack_resources()
        .stack_name("dsr-stack")
        .send()
        .await
        .unwrap();

    let resources = result.stack_resources();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].logical_resource_id(), Some("MyQueue"));
    assert_eq!(resources[0].resource_type(), Some("AWS::SQS::Queue"));
}

// ---------------------------------------------------------------------------
// UpdateStack
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "UpdateStack", checksum = "6aeeb381")]
#[tokio::test]
async fn cloudformation_update_stack() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    client
        .create_stack()
        .stack_name("update-stack")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();

    let template_v2 = r#"{
        "Resources": {
            "NewQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-conf-queue-updated" }
            }
        }
    }"#;

    client
        .update_stack()
        .stack_name("update-stack")
        .template_body(template_v2)
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_stacks()
        .stack_name("update-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.stacks()[0].stack_status().map(|s| s.as_str()),
        Some("UPDATE_COMPLETE")
    );
}

// ---------------------------------------------------------------------------
// GetTemplate
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "GetTemplate", checksum = "61885956")]
#[tokio::test]
async fn cloudformation_get_template() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    let template = r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cf-gt-queue"}}}}"#;

    client
        .create_stack()
        .stack_name("gt-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = client
        .get_template()
        .stack_name("gt-stack")
        .send()
        .await
        .unwrap();

    let body = result.template_body().unwrap();
    assert!(body.contains("AWS::SQS::Queue"));
    assert!(body.contains("cf-gt-queue"));
}

// ── Conformance closure batch (all 82 missing CFN ops covered by raw POSTs) ──

const CFN_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=0";

fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_' || b == b'~' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

async fn cfn_post(server: &TestServer, action: &str, params: &[(&str, &str)]) -> reqwest::Response {
    let mut body = format!("Action={action}&Version=2010-05-15");
    for (k, v) in params {
        body.push_str(&format!("&{}={}", pct(k), pct(v)));
    }
    reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-www-form-urlencoded")
        .header("Authorization", CFN_AUTH)
        .body(body)
        .send()
        .await
        .unwrap()
}

#[test_action("cloudformation", "ActivateOrganizationsAccess", checksum = "a909ded9")]
#[test_action("cloudformation", "ActivateType", checksum = "db4fc41c")]
#[test_action(
    "cloudformation",
    "BatchDescribeTypeConfigurations",
    checksum = "7878ad70"
)]
#[test_action("cloudformation", "CancelUpdateStack", checksum = "2ea0802c")]
#[test_action("cloudformation", "ContinueUpdateRollback", checksum = "df1d3017")]
#[test_action("cloudformation", "CreateChangeSet", checksum = "0ad063e9")]
#[test_action("cloudformation", "CreateGeneratedTemplate", checksum = "1f77f497")]
#[test_action("cloudformation", "CreateStackInstances", checksum = "e31e799a")]
#[test_action("cloudformation", "CreateStackRefactor", checksum = "316d91f3")]
#[test_action("cloudformation", "CreateStackSet", checksum = "2863e057")]
#[test_action(
    "cloudformation",
    "DeactivateOrganizationsAccess",
    checksum = "28a7b779"
)]
#[test_action("cloudformation", "DeactivateType", checksum = "1df9bf2a")]
#[test_action("cloudformation", "DeleteChangeSet", checksum = "5c84e165")]
#[test_action("cloudformation", "DeleteGeneratedTemplate", checksum = "505b0044")]
#[test_action("cloudformation", "DeleteStackInstances", checksum = "1bad46e1")]
#[test_action("cloudformation", "DeleteStackSet", checksum = "03dbedcc")]
#[test_action("cloudformation", "DeregisterType", checksum = "0655643d")]
#[test_action("cloudformation", "DescribeAccountLimits", checksum = "59b20123")]
#[test_action("cloudformation", "DescribeChangeSet", checksum = "0056240c")]
#[test_action("cloudformation", "DescribeChangeSetHooks", checksum = "02683306")]
#[test_action("cloudformation", "DescribeEvents", checksum = "c27983c2")]
#[test_action("cloudformation", "DescribeGeneratedTemplate", checksum = "b597597b")]
#[test_action("cloudformation", "DescribeOrganizationsAccess", checksum = "263cfe22")]
#[test_action("cloudformation", "DescribePublisher", checksum = "90637731")]
#[test_action("cloudformation", "DescribeResourceScan", checksum = "682b6d6d")]
#[test_action(
    "cloudformation",
    "DescribeStackDriftDetectionStatus",
    checksum = "aab0d319"
)]
#[test_action("cloudformation", "DescribeStackEvents", checksum = "fb134cf9")]
#[test_action("cloudformation", "DescribeStackInstance", checksum = "55ef484e")]
#[test_action("cloudformation", "DescribeStackRefactor", checksum = "3bd4af49")]
#[test_action("cloudformation", "DescribeStackResource", checksum = "c4c2f783")]
#[test_action("cloudformation", "DescribeStackResourceDrifts", checksum = "8bd6482e")]
#[test_action("cloudformation", "DescribeStackSet", checksum = "cba24b13")]
#[test_action("cloudformation", "DescribeStackSetOperation", checksum = "0a6c7980")]
#[test_action("cloudformation", "DescribeType", checksum = "44c82bd8")]
#[test_action("cloudformation", "DescribeTypeRegistration", checksum = "e219d9d8")]
#[test_action("cloudformation", "DetectStackDrift", checksum = "a0da8d6f")]
#[test_action("cloudformation", "DetectStackResourceDrift", checksum = "e861aafb")]
#[test_action("cloudformation", "DetectStackSetDrift", checksum = "80e6fe49")]
#[test_action("cloudformation", "EstimateTemplateCost", checksum = "c31a77ec")]
#[test_action("cloudformation", "ExecuteChangeSet", checksum = "de1105f7")]
#[test_action("cloudformation", "ExecuteStackRefactor", checksum = "971e47f5")]
#[test_action("cloudformation", "GetGeneratedTemplate", checksum = "744a0498")]
#[test_action("cloudformation", "GetHookResult", checksum = "8bd5d431")]
#[test_action("cloudformation", "GetStackPolicy", checksum = "afc542d0")]
#[test_action("cloudformation", "GetTemplateSummary", checksum = "0a3129bd")]
#[test_action("cloudformation", "ImportStacksToStackSet", checksum = "cfa57349")]
#[test_action("cloudformation", "ListChangeSets", checksum = "15fbb1c5")]
#[test_action("cloudformation", "ListExports", checksum = "deb44896")]
#[test_action("cloudformation", "ListGeneratedTemplates", checksum = "7ef78547")]
#[test_action("cloudformation", "ListHookResults", checksum = "3a502a56")]
#[test_action("cloudformation", "ListImports", checksum = "7788a96e")]
#[test_action(
    "cloudformation",
    "ListResourceScanRelatedResources",
    checksum = "6b12e80e"
)]
#[test_action("cloudformation", "ListResourceScanResources", checksum = "c7208861")]
#[test_action("cloudformation", "ListResourceScans", checksum = "c424ed88")]
#[test_action(
    "cloudformation",
    "ListStackInstanceResourceDrifts",
    checksum = "3fedc547"
)]
#[test_action("cloudformation", "ListStackInstances", checksum = "12bb6d65")]
#[test_action("cloudformation", "ListStackRefactorActions", checksum = "d6c00ba2")]
#[test_action("cloudformation", "ListStackRefactors", checksum = "25f50c60")]
#[test_action(
    "cloudformation",
    "ListStackSetAutoDeploymentTargets",
    checksum = "7d61bc94"
)]
#[test_action(
    "cloudformation",
    "ListStackSetOperationResults",
    checksum = "a2c89af0"
)]
#[test_action("cloudformation", "ListStackSetOperations", checksum = "ebd93177")]
#[test_action("cloudformation", "ListStackSets", checksum = "f2550c96")]
#[test_action("cloudformation", "ListTypeRegistrations", checksum = "84ab6efc")]
#[test_action("cloudformation", "ListTypeVersions", checksum = "5357f950")]
#[test_action("cloudformation", "ListTypes", checksum = "d940b3b8")]
#[test_action("cloudformation", "PublishType", checksum = "59db2d85")]
#[test_action("cloudformation", "RecordHandlerProgress", checksum = "9fc527f8")]
#[test_action("cloudformation", "RegisterPublisher", checksum = "0ffee522")]
#[test_action("cloudformation", "RegisterType", checksum = "ac98b9ac")]
#[test_action("cloudformation", "RollbackStack", checksum = "5ff4caea")]
#[test_action("cloudformation", "SetStackPolicy", checksum = "f5625560")]
#[test_action("cloudformation", "SetTypeConfiguration", checksum = "1b0cc5f5")]
#[test_action("cloudformation", "SetTypeDefaultVersion", checksum = "deba6bf5")]
#[test_action("cloudformation", "SignalResource", checksum = "40529783")]
#[test_action("cloudformation", "StartResourceScan", checksum = "67f52512")]
#[test_action("cloudformation", "StopStackSetOperation", checksum = "249d33dc")]
#[test_action("cloudformation", "TestType", checksum = "a70045e1")]
#[test_action("cloudformation", "UpdateGeneratedTemplate", checksum = "4f3355a4")]
#[test_action("cloudformation", "UpdateStackInstances", checksum = "346e0e7e")]
#[test_action("cloudformation", "UpdateStackSet", checksum = "15c71d2f")]
#[test_action("cloudformation", "UpdateTerminationProtection", checksum = "db98c3f9")]
#[test_action("cloudformation", "ValidateTemplate", checksum = "ef7752b7")]
#[tokio::test]
async fn cloudformation_closure_routes_exist() {
    // Every route added in this PR is exercised below. We assert HTTP 2xx
    // (route hit + handler succeeded). Each `#[test_action]` above pins
    // the operation to its Smithy checksum so the audit knows it has
    // coverage even when the test groups multiple ops together.
    let server = TestServer::start().await;

    // Change sets
    assert!(cfn_post(
        &server,
        "CreateChangeSet",
        &[("StackName", "s1"), ("ChangeSetName", "cs1")]
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "DescribeChangeSet", &[("ChangeSetName", "cs1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(
        &server,
        "DescribeChangeSetHooks",
        &[("ChangeSetName", "cs1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(&server, "ListChangeSets", &[("StackName", "s1")])
        .await
        .status()
        .is_success());
    assert!(
        cfn_post(&server, "ExecuteChangeSet", &[("ChangeSetName", "cs1")])
            .await
            .status()
            .is_success()
    );
    assert!(
        cfn_post(&server, "DeleteChangeSet", &[("ChangeSetName", "cs1")])
            .await
            .status()
            .is_success()
    );

    // Stack sets
    assert!(
        cfn_post(&server, "CreateStackSet", &[("StackSetName", "ss1")])
            .await
            .status()
            .is_success()
    );
    assert!(
        cfn_post(&server, "DescribeStackSet", &[("StackSetName", "ss1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(&server, "ListStackSets", &[])
        .await
        .status()
        .is_success());
    assert!(
        cfn_post(&server, "UpdateStackSet", &[("StackSetName", "ss1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(
        &server,
        "DescribeStackSetOperation",
        &[("StackSetName", "ss1"), ("OperationId", "op1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ListStackSetOperations",
        &[("StackSetName", "ss1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ListStackSetOperationResults",
        &[("StackSetName", "ss1"), ("OperationId", "op1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ListStackSetAutoDeploymentTargets",
        &[("StackSetName", "ss1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "StopStackSetOperation",
        &[("StackSetName", "ss1"), ("OperationId", "op1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ImportStacksToStackSet",
        &[("StackSetName", "ss1")]
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "DeleteStackSet", &[("StackSetName", "ss1")])
            .await
            .status()
            .is_success()
    );

    // Stack instances
    assert!(cfn_post(
        &server,
        "CreateStackInstances",
        &[("StackSetName", "ss1"), ("Regions.member.1", "us-east-1"),],
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "UpdateStackInstances",
        &[("StackSetName", "ss1"), ("Regions.member.1", "us-east-1"),],
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DeleteStackInstances",
        &[
            ("StackSetName", "ss1"),
            ("Regions.member.1", "us-east-1"),
            ("RetainStacks", "false"),
        ],
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DescribeStackInstance",
        &[
            ("StackSetName", "ss1"),
            ("StackInstanceAccount", "000000000000"),
            ("StackInstanceRegion", "us-east-1"),
        ],
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "ListStackInstances", &[("StackSetName", "ss1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(
        &server,
        "ListStackInstanceResourceDrifts",
        &[
            ("StackSetName", "ss1"),
            ("StackInstanceAccount", "000000000000"),
            ("StackInstanceRegion", "us-east-1"),
            ("OperationId", "op1"),
        ],
    )
    .await
    .status()
    .is_success());

    // Refactors
    assert!(cfn_post(
        &server,
        "CreateStackRefactor",
        &[("StackDefinitions.member.1.StackName", "s1")],
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DescribeStackRefactor",
        &[("StackRefactorId", "r1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ExecuteStackRefactor",
        &[("StackRefactorId", "r1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(&server, "ListStackRefactors", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "ListStackRefactorActions",
        &[("StackRefactorId", "r1")]
    )
    .await
    .status()
    .is_success());

    // Types
    assert!(cfn_post(&server, "ActivateType", &[("Type", "RESOURCE")])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "DeactivateType",
        &[("TypeName", "AWS::Demo::Type")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(&server, "DescribeType", &[("Type", "RESOURCE")])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "DescribeTypeRegistration",
        &[("RegistrationToken", "tok1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "RegisterType",
        &[
            ("Type", "RESOURCE"),
            ("TypeName", "AWS::Demo::Type"),
            ("SchemaHandlerPackage", "s3://x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DeregisterType",
        &[("TypeName", "AWS::Demo::Type")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(&server, "ListTypes", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(&server, "ListTypeRegistrations", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "ListTypeVersions",
        &[("TypeName", "AWS::Demo::Type")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "BatchDescribeTypeConfigurations",
        &[("TypeConfigurationIdentifiers.member.1.Type", "RESOURCE")],
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "SetTypeConfiguration", &[("Configuration", "{}")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(
        &server,
        "SetTypeDefaultVersion",
        &[("TypeName", "AWS::Demo::Type")]
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "TestType", &[("TypeName", "AWS::Demo::Type")])
            .await
            .status()
            .is_success()
    );
    assert!(
        cfn_post(&server, "PublishType", &[("TypeName", "AWS::Demo::Type")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(&server, "RegisterPublisher", &[])
        .await
        .status()
        .is_success());
    assert!(
        cfn_post(&server, "DescribePublisher", &[("PublisherId", "p1")])
            .await
            .status()
            .is_success()
    );

    // Generated templates
    assert!(cfn_post(
        &server,
        "CreateGeneratedTemplate",
        &[("GeneratedTemplateName", "gt1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "UpdateGeneratedTemplate",
        &[("GeneratedTemplateName", "gt1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DescribeGeneratedTemplate",
        &[("GeneratedTemplateName", "gt1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "GetGeneratedTemplate",
        &[("GeneratedTemplateName", "gt1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(&server, "ListGeneratedTemplates", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "DeleteGeneratedTemplate",
        &[("GeneratedTemplateName", "gt1")]
    )
    .await
    .status()
    .is_success());

    // Resource scans
    assert!(cfn_post(&server, "StartResourceScan", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "DescribeResourceScan",
        &[("ResourceScanId", "rs1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(&server, "ListResourceScans", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "ListResourceScanResources",
        &[("ResourceScanId", "rs1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ListResourceScanRelatedResources",
        &[
            ("ResourceScanId", "rs1"),
            ("Resources.member.1.ResourceType", "AWS::SQS::Queue"),
        ],
    )
    .await
    .status()
    .is_success());

    // Drift detection
    assert!(
        cfn_post(&server, "DetectStackDrift", &[("StackName", "s1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(
        &server,
        "DetectStackResourceDrift",
        &[("StackName", "s1"), ("LogicalResourceId", "L")]
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "DetectStackSetDrift", &[("StackSetName", "ss1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(
        &server,
        "DescribeStackDriftDetectionStatus",
        &[("StackDriftDetectionId", "d1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DescribeStackResourceDrifts",
        &[("StackName", "s1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "DescribeStackResource",
        &[("StackName", "s1"), ("LogicalResourceId", "L")]
    )
    .await
    .status()
    .is_success());

    // Events
    assert!(
        cfn_post(&server, "DescribeStackEvents", &[("StackName", "s1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(&server, "DescribeEvents", &[("StackName", "s1")])
        .await
        .status()
        .is_success());

    // Hooks
    assert!(cfn_post(
        &server,
        "GetHookResult",
        &[
            ("HookId", "h1"),
            ("InvocationPoint", "PRE_PROVISION"),
            ("StackName", "s1")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "ListHookResults",
        &[("TargetType", "STACK"), ("TargetId", "s1")]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "RecordHandlerProgress",
        &[("BearerToken", "bt"), ("OperationStatus", "SUCCESS")]
    )
    .await
    .status()
    .is_success());

    // Imports / exports
    assert!(cfn_post(&server, "ListExports", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(&server, "ListImports", &[("ExportName", "x")])
        .await
        .status()
        .is_success());

    // Stack policies
    assert!(cfn_post(&server, "GetStackPolicy", &[("StackName", "s1")])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "SetStackPolicy",
        &[("StackName", "s1"), ("StackPolicyBody", "{}")]
    )
    .await
    .status()
    .is_success());

    // Termination protection
    assert!(cfn_post(
        &server,
        "UpdateTerminationProtection",
        &[("StackName", "s1"), ("EnableTerminationProtection", "true")]
    )
    .await
    .status()
    .is_success());

    // Account / org / utilities
    assert!(cfn_post(&server, "DescribeAccountLimits", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(&server, "ActivateOrganizationsAccess", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(&server, "DeactivateOrganizationsAccess", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(&server, "DescribeOrganizationsAccess", &[])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "ValidateTemplate",
        &[("TemplateBody", SIMPLE_TEMPLATE)]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "EstimateTemplateCost",
        &[("TemplateBody", SIMPLE_TEMPLATE)]
    )
    .await
    .status()
    .is_success());
    assert!(cfn_post(
        &server,
        "GetTemplateSummary",
        &[("TemplateBody", SIMPLE_TEMPLATE)]
    )
    .await
    .status()
    .is_success());
    assert!(
        cfn_post(&server, "CancelUpdateStack", &[("StackName", "s1")])
            .await
            .status()
            .is_success()
    );
    assert!(
        cfn_post(&server, "ContinueUpdateRollback", &[("StackName", "s1")])
            .await
            .status()
            .is_success()
    );
    assert!(cfn_post(&server, "RollbackStack", &[("StackName", "s1")])
        .await
        .status()
        .is_success());
    assert!(cfn_post(
        &server,
        "SignalResource",
        &[
            ("StackName", "s1"),
            ("LogicalResourceId", "L"),
            ("UniqueId", "u"),
            ("Status", "SUCCESS")
        ]
    )
    .await
    .status()
    .is_success());
}
