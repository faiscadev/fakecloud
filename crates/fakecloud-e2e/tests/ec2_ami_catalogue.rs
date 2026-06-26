//! Seeded public AMI catalogue behavior: the amazon/Canonical-owned seeds are
//! read-only (DeregisterImage / ModifyImageAttribute on them is AuthFailure, as
//! in real AWS), the catalogue survives an upgrade-then-restart in persistent
//! mode, and ReplaceImageCriteriaInAllowedImagesSettings round-trips.

mod helpers;

use helpers::TestServer;

async fn an_amazon_ami(c: &aws_sdk_ec2::Client) -> String {
    c.describe_images()
        .owners("amazon")
        .send()
        .await
        .unwrap()
        .images()
        .first()
        .expect("seeded amazon AMI present")
        .image_id()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn seeded_public_amis_are_read_only() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let seed = an_amazon_ami(&c).await;

    // Deregister of an amazon-owned seed must be rejected (AuthFailure).
    let err = c
        .deregister_image()
        .image_id(&seed)
        .send()
        .await
        .expect_err("deregister of amazon AMI should fail");
    assert!(
        format!("{err:?}").contains("AuthFailure"),
        "expected AuthFailure, got {err:?}"
    );

    // ModifyImageAttribute on a seed must also be rejected.
    let err = c
        .modify_image_attribute()
        .image_id(&seed)
        .description(
            aws_sdk_ec2::types::AttributeValue::builder()
                .value("hijack")
                .build(),
        )
        .send()
        .await
        .expect_err("modify of amazon AMI should fail");
    assert!(format!("{err:?}").contains("AuthFailure"), "got {err:?}");

    // The seed is still present + unmodified.
    let still = c.describe_images().image_ids(&seed).send().await.unwrap();
    assert_eq!(
        still.images().len(),
        1,
        "seed must survive the rejected ops"
    );

    // A user-registered AMI (owned by the caller) CAN be deregistered.
    let mine = c
        .register_image()
        .name("my-own-ami")
        .send()
        .await
        .unwrap()
        .image_id()
        .unwrap()
        .to_string();
    c.deregister_image()
        .image_id(&mine)
        .send()
        .await
        .expect("deregister of own AMI should succeed");
}

#[tokio::test]
async fn replace_image_criteria_round_trips() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    c.replace_image_criteria_in_allowed_images_settings()
        .image_criteria(
            aws_sdk_ec2::types::ImageCriterionRequest::builder()
                .image_providers("amazon")
                .image_providers("123456789012")
                .build(),
        )
        .send()
        .await
        .expect("replace criteria");

    let got = c
        .get_allowed_images_settings()
        .send()
        .await
        .expect("get settings");
    let criteria = got.image_criteria();
    assert_eq!(criteria.len(), 1, "one criterion persisted");
    let providers = criteria[0].image_providers();
    assert!(
        providers.contains(&"amazon".to_string())
            && providers.contains(&"123456789012".to_string()),
        "providers round-trip: {providers:?}"
    );
}

#[tokio::test]
async fn ami_catalogue_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = TestServer::start_persistent(tmp.path()).await;

    // Touch EC2 so the account is persisted.
    let c = s.ec2_client().await;
    c.create_vpc()
        .cidr_block("10.9.0.0/16")
        .send()
        .await
        .unwrap();
    let before = c
        .describe_images()
        .owners("amazon")
        .send()
        .await
        .unwrap()
        .images()
        .len();
    assert!(before >= 4, "seeds present before restart: {before}");

    s.restart().await;

    let c2 = s.ec2_client().await;
    let after = c2
        .describe_images()
        .owners("amazon")
        .send()
        .await
        .unwrap()
        .images()
        .len();
    assert_eq!(after, before, "amazon AMI catalogue must survive restart");
}
