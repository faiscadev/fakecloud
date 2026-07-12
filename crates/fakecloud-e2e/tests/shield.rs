//! AWS Shield / Shield Advanced control-plane E2E.
//!
//! Exercises the Shield Advanced control plane against a spawned fakecloud
//! server via the AWS Rust SDK, which speaks the real awsJson1.1 wire format
//! (x-amz-target `AWSShield_20160616.<Op>`):
//!
//!   CreateSubscription -> GetSubscriptionState (ACTIVE)
//!     -> CreateProtection -> DescribeProtection / ListProtections
//!     -> CreateProtectionGroup -> ListResourcesInProtectionGroup
//!     -> ListAttacks (empty)
//!     -> UpdateEmergencyContactSettings / DescribeEmergencyContactSettings
//!     -> TagResource / ListTagsForResource
//!     -> DeleteProtection
//!
//! Pure CONTROL-PLANE test: every operation is real, validated, persisted CRUD.
//! Attack surfacing is honest -- `ListAttacks` returns an empty list because no
//! synthetic DDoS records are ever fabricated.

mod helpers;

use aws_sdk_shield::types::{EmergencyContact, Tag};
use helpers::TestServer;

async fn shield_client(server: &TestServer) -> aws_sdk_shield::Client {
    aws_sdk_shield::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn shield_advanced_control_plane_lifecycle() {
    let server = TestServer::start().await;
    let shield = shield_client(&server).await;

    // Before subscribing, the account is INACTIVE.
    let state = shield
        .get_subscription_state()
        .send()
        .await
        .expect("get subscription state");
    assert_eq!(state.subscription_state().as_str(), "INACTIVE");

    // CreateSubscription starts a one-year Shield Advanced subscription.
    shield
        .create_subscription()
        .send()
        .await
        .expect("create subscription");
    let state = shield
        .get_subscription_state()
        .send()
        .await
        .expect("get subscription state after subscribe");
    assert_eq!(state.subscription_state().as_str(), "ACTIVE");

    // DescribeSubscription returns the full limits.
    let sub = shield
        .describe_subscription()
        .send()
        .await
        .expect("describe subscription");
    let subscription = sub.subscription().expect("subscription present");
    assert!(subscription.subscription_limits().is_some());

    // CreateProtection for a CloudFront distribution.
    let resource_arn = "arn:aws:cloudfront::123456789012:distribution/E1EXAMPLE";
    let created = shield
        .create_protection()
        .name("web-frontend")
        .resource_arn(resource_arn)
        .send()
        .await
        .expect("create protection");
    let protection_id = created.protection_id().expect("protection id").to_string();
    assert_eq!(protection_id.len(), 36, "36-char ProtectionId expected");

    // DescribeProtection round-trips by id.
    let described = shield
        .describe_protection()
        .protection_id(&protection_id)
        .send()
        .await
        .expect("describe protection");
    let protection = described.protection().expect("protection present");
    assert_eq!(protection.resource_arn(), Some(resource_arn));
    let protection_arn = protection
        .protection_arn()
        .expect("protection arn")
        .to_string();
    assert!(
        protection_arn.contains(":shield::") && protection_arn.contains(":protection/"),
        "unexpected protection ARN: {protection_arn}"
    );

    // A second protection for the same resource is rejected.
    let dup = shield
        .create_protection()
        .name("dup")
        .resource_arn(resource_arn)
        .send()
        .await;
    assert!(dup.is_err(), "duplicate resource protection should error");

    // ListProtections sees it.
    let listed = shield
        .list_protections()
        .send()
        .await
        .expect("list protections");
    assert!(
        listed
            .protections()
            .iter()
            .any(|p| p.id() == Some(protection_id.as_str())),
        "protection should appear in ListProtections"
    );

    // CreateProtectionGroup aggregating all protections.
    shield
        .create_protection_group()
        .protection_group_id("frontend")
        .aggregation(aws_sdk_shield::types::ProtectionGroupAggregation::Sum)
        .pattern(aws_sdk_shield::types::ProtectionGroupPattern::All)
        .send()
        .await
        .expect("create protection group");
    let group = shield
        .describe_protection_group()
        .protection_group_id("frontend")
        .send()
        .await
        .expect("describe protection group");
    assert_eq!(
        group
            .protection_group()
            .map(|g| g.aggregation().as_str().to_string()),
        Some("SUM".to_string())
    );
    let resources = shield
        .list_resources_in_protection_group()
        .protection_group_id("frontend")
        .send()
        .await
        .expect("list resources in protection group");
    assert!(
        resources.resource_arns().is_empty(),
        "an ALL-pattern group lists no explicit members"
    );

    // ListAttacks is honestly empty (no synthetic DDoS records).
    let attacks = shield.list_attacks().send().await.expect("list attacks");
    assert!(
        attacks.attack_summaries().is_empty(),
        "no synthetic attacks should be surfaced"
    );

    // Emergency-contact settings round-trip.
    shield
        .update_emergency_contact_settings()
        .emergency_contact_list(
            EmergencyContact::builder()
                .email_address("secops@example.com")
                .build()
                .expect("emergency contact"),
        )
        .send()
        .await
        .expect("update emergency contact settings");
    let contacts = shield
        .describe_emergency_contact_settings()
        .send()
        .await
        .expect("describe emergency contact settings");
    assert_eq!(
        contacts
            .emergency_contact_list()
            .first()
            .map(|c| c.email_address()),
        Some("secops@example.com")
    );

    // Tag the protection, then read tags back.
    shield
        .tag_resource()
        .resource_arn(&protection_arn)
        .tags(Tag::builder().key("env").value("e2e").build())
        .send()
        .await
        .expect("tag resource");
    let tags = shield
        .list_tags_for_resource()
        .resource_arn(&protection_arn)
        .send()
        .await
        .expect("list tags for resource");
    assert!(
        tags.tags()
            .iter()
            .any(|t| t.key() == Some("env") && t.value() == Some("e2e")),
        "tag should be listed"
    );

    // DeleteProtection removes it.
    shield
        .delete_protection()
        .protection_id(&protection_id)
        .send()
        .await
        .expect("delete protection");
    let after = shield
        .describe_protection()
        .protection_id(&protection_id)
        .send()
        .await;
    assert!(after.is_err(), "deleted protection should not be found");
}
