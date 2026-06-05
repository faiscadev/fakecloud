//! Handwritten Level-2 conformance tests for EC2 (ec2Query protocol).
//!
//! One `#[test_action]` per declared `SUPPORTED_ACTIONS` entry — the audit
//! cross-checks this list against the service crate and fails the build on any
//! gap. Grows one resource-family batch at a time toward full op parity.

#![recursion_limit = "512"]

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("ec2", "DescribeTags", checksum = "aa62d5db")]
#[tokio::test]
async fn ec2_describe_tags() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    // On a fresh account DescribeTags returns an empty, well-formed tagSet —
    // the ec2Query flattened-list deserialization path the SDK exercises.
    // (Round-trip create+describe coverage lands with CreateTags in the next
    // batch, once the L1 probe can encode input-bearing ec2Query ops.)
    let described = client.describe_tags().send().await.unwrap();
    assert!(described.tags().is_empty());

    // A resource-id filter on an empty store also returns an empty set.
    let filtered = client
        .describe_tags()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("resource-id")
                .values("subnet-0123456789abcdef0")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(filtered.tags().is_empty());
}

#[test_action("ec2", "DescribeRegions", checksum = "a618a443")]
#[tokio::test]
async fn ec2_describe_regions() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    let response = client.describe_regions().send().await.unwrap();
    let regions = response.regions();
    assert!(regions.len() >= 15);
    let us_east_1 = regions
        .iter()
        .find(|r| r.region_name() == Some("us-east-1"))
        .expect("us-east-1 present");
    assert_eq!(us_east_1.endpoint(), Some("ec2.us-east-1.amazonaws.com"));

    // Explicit region-name filter narrows the result.
    let filtered = client
        .describe_regions()
        .region_names("eu-west-1")
        .send()
        .await
        .unwrap();
    assert_eq!(filtered.regions().len(), 1);
    assert_eq!(filtered.regions()[0].region_name(), Some("eu-west-1"));
}

#[test_action("ec2", "DescribeAvailabilityZones", checksum = "375077ee")]
#[tokio::test]
async fn ec2_describe_availability_zones() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    let response = client.describe_availability_zones().send().await.unwrap();
    let zones = response.availability_zones();
    assert_eq!(zones.len(), 3);
    assert!(zones.iter().all(|z| z.region_name() == Some("us-east-1")));
    assert!(zones.iter().any(|z| z.zone_name() == Some("us-east-1a")));
    let a = zones
        .iter()
        .find(|z| z.zone_name() == Some("us-east-1a"))
        .unwrap();
    assert_eq!(a.zone_id(), Some("use1-az1"));
}

#[test_action("ec2", "DescribeAccountAttributes", checksum = "62e5ea8d")]
#[tokio::test]
async fn ec2_describe_account_attributes() {
    let server = TestServer::start().await;
    let client = server.ec2_client().await;

    let response = client.describe_account_attributes().send().await.unwrap();
    let attrs = response.account_attributes();
    let supported = attrs
        .iter()
        .find(|a| a.attribute_name() == Some("supported-platforms"))
        .expect("supported-platforms present");
    assert!(supported
        .attribute_values()
        .iter()
        .any(|v| v.attribute_value() == Some("VPC")));
}
