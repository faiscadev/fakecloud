mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

type Client = aws_sdk_resourcegroupstagging::Client;

#[test_action("tagging", "TagResources", checksum = "540a1974")]
#[test_action("tagging", "GetResources", checksum = "1ce39de8")]
#[test_action("tagging", "GetTagKeys", checksum = "96e82838")]
#[test_action("tagging", "GetTagValues", checksum = "4314e9a8")]
#[test_action("tagging", "UntagResources", checksum = "39f1946f")]
#[test_action("tagging", "GetComplianceSummary", checksum = "f7ec9085")]
#[test_action("tagging", "StartReportCreation", checksum = "59de8c3c")]
#[test_action("tagging", "DescribeReportCreation", checksum = "ecdddbec")]
#[test_action("tagging", "ListRequiredTags", checksum = "65cd6a9a")]
#[tokio::test]
async fn tagging_lifecycle() {
    let server = TestServer::start().await;
    let client: Client = server.resourcegroupstaggingapi_client().await;

    let arn = "arn:aws:custom:us-east-1:000000000000:thing/abc";

    // Tag an arbitrary ARN, then it shows up in GetResources with its tags.
    client
        .tag_resources()
        .resource_arn_list(arn)
        .tags("stage", "prod")
        .tags("team", "web")
        .send()
        .await
        .unwrap();

    let resources = client.get_resources().send().await.unwrap();
    let list = resources.resource_tag_mapping_list();
    assert!(list.iter().any(|m| m.resource_arn() == Some(arn)));

    // Tag keys / values reflect the applied tags.
    let keys = client.get_tag_keys().send().await.unwrap();
    assert!(keys.tag_keys().contains(&"stage".to_string()));

    let values = client.get_tag_values().key("stage").send().await.unwrap();
    assert!(values.tag_values().contains(&"prod".to_string()));

    // Filter by tag.
    let filtered = client
        .get_resources()
        .tag_filters(
            aws_sdk_resourcegroupstagging::types::TagFilter::builder()
                .key("stage")
                .values("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(filtered
        .resource_tag_mapping_list()
        .iter()
        .any(|m| m.resource_arn() == Some(arn)));

    // Untag removes the key.
    client
        .untag_resources()
        .resource_arn_list(arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();
    let after = client.get_tag_keys().send().await.unwrap();
    assert!(!after.tag_keys().contains(&"team".to_string()));

    // Compliance + report ops respond.
    client.get_compliance_summary().send().await.unwrap();
    client
        .start_report_creation()
        .s3_bucket("my-bucket")
        .send()
        .await
        .unwrap();
    let report = client.describe_report_creation().send().await.unwrap();
    assert_eq!(report.status(), Some("SUCCEEDED"));

    client.list_required_tags().send().await.unwrap();
}
