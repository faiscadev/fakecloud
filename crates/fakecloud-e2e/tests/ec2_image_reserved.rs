//! EC2 round-trip E2E for image attributes and reserved-instance
//! listings/modifications — proving writes persist and reflect on read
//! (bug-hunt 2026-06-13 findings 1.2 and 1.5).

mod helpers;

use helpers::TestServer;

async fn make_ami(c: &aws_sdk_ec2::Client) -> String {
    c.register_image()
        .name("ami-roundtrip")
        .send()
        .await
        .unwrap()
        .image_id()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn modify_image_attribute_launch_permission_round_trips() {
    use aws_sdk_ec2::types::{LaunchPermission, LaunchPermissionModifications, OperationType};
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;

    // Add a cross-account share.
    c.modify_image_attribute()
        .image_id(&id)
        .launch_permission(
            LaunchPermissionModifications::builder()
                .add(LaunchPermission::builder().user_id("111122223333").build())
                .add(LaunchPermission::builder().user_id("444455556666").build())
                .build(),
        )
        .send()
        .await
        .unwrap();

    // DescribeImageAttribute must reflect the added permissions.
    let r = c
        .describe_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ImageAttributeName::LaunchPermission)
        .send()
        .await
        .unwrap();
    let users: Vec<&str> = r
        .launch_permissions()
        .iter()
        .filter_map(|p| p.user_id())
        .collect();
    assert!(users.contains(&"111122223333"), "users={users:?}");
    assert!(users.contains(&"444455556666"), "users={users:?}");

    // Remove one.
    c.modify_image_attribute()
        .image_id(&id)
        .launch_permission(
            LaunchPermissionModifications::builder()
                .remove(LaunchPermission::builder().user_id("111122223333").build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let r = c
        .describe_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ImageAttributeName::LaunchPermission)
        .send()
        .await
        .unwrap();
    let users: Vec<&str> = r
        .launch_permissions()
        .iter()
        .filter_map(|p| p.user_id())
        .collect();
    assert!(!users.contains(&"111122223333"), "users={users:?}");
    assert!(users.contains(&"444455556666"), "users={users:?}");

    // The `all` group makes the AMI public.
    c.modify_image_attribute()
        .image_id(&id)
        .operation_type(OperationType::Add)
        .launch_permission(
            LaunchPermissionModifications::builder()
                .add(LaunchPermission::builder().group("all".into()).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let imgs = c.describe_images().image_ids(&id).send().await.unwrap();
    assert_eq!(imgs.images()[0].public(), Some(true));

    // ResetImageAttribute clears launch permissions.
    c.reset_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ResetImageAttributeName::LaunchPermission)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ImageAttributeName::LaunchPermission)
        .send()
        .await
        .unwrap();
    assert!(r.launch_permissions().is_empty());
}

#[tokio::test]
async fn modify_image_attribute_description_round_trips() {
    use aws_sdk_ec2::types::AttributeValue;
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = make_ami(&c).await;

    c.modify_image_attribute()
        .image_id(&id)
        .description(AttributeValue::builder().value("a new description").build())
        .send()
        .await
        .unwrap();
    let r = c
        .describe_image_attribute()
        .image_id(&id)
        .attribute(aws_sdk_ec2::types::ImageAttributeName::Description)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.description().and_then(|d| d.value()),
        Some("a new description")
    );
}

#[tokio::test]
async fn reserved_instances_listings_persist_and_describe() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    // Purchase an RI first so the listing references a real id.
    let ri = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id("offering-1")
        .instance_count(1)
        .send()
        .await
        .unwrap()
        .reserved_instances_id()
        .unwrap()
        .to_string();

    let created = c
        .create_reserved_instances_listing()
        .reserved_instances_id(&ri)
        .instance_count(1)
        .client_token("tok-1")
        .send()
        .await
        .unwrap();
    let listing_id = created.reserved_instances_listings()[0]
        .reserved_instances_listing_id()
        .unwrap()
        .to_string();

    // DescribeReservedInstancesListings must return the created listing.
    let described = c
        .describe_reserved_instances_listings()
        .send()
        .await
        .unwrap();
    let ids: Vec<&str> = described
        .reserved_instances_listings()
        .iter()
        .filter_map(|l| l.reserved_instances_listing_id())
        .collect();
    assert!(ids.contains(&listing_id.as_str()), "ids={ids:?}");

    // Filter by listing id.
    let filtered = c
        .describe_reserved_instances_listings()
        .reserved_instances_listing_id(&listing_id)
        .send()
        .await
        .unwrap();
    assert_eq!(filtered.reserved_instances_listings().len(), 1);

    // Cancel flips status to cancelled and persists.
    c.cancel_reserved_instances_listing()
        .reserved_instances_listing_id(&listing_id)
        .send()
        .await
        .unwrap();
    let described = c
        .describe_reserved_instances_listings()
        .reserved_instances_listing_id(&listing_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.reserved_instances_listings()[0].status().map(|s| s.as_str()),
        Some("cancelled")
    );
}

#[tokio::test]
async fn reserved_instances_modifications_persist_and_describe() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let ri = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id("offering-1")
        .instance_count(1)
        .send()
        .await
        .unwrap()
        .reserved_instances_id()
        .unwrap()
        .to_string();

    let modified = c
        .modify_reserved_instances()
        .reserved_instances_ids(&ri)
        .client_token("modtok")
        .send()
        .await
        .unwrap();
    let mod_id = modified
        .reserved_instances_modification_id()
        .unwrap()
        .to_string();

    let described = c
        .describe_reserved_instances_modifications()
        .send()
        .await
        .unwrap();
    let ids: Vec<&str> = described
        .reserved_instances_modifications()
        .iter()
        .filter_map(|m| m.reserved_instances_modification_id())
        .collect();
    assert!(ids.contains(&mod_id.as_str()), "ids={ids:?}");
}
