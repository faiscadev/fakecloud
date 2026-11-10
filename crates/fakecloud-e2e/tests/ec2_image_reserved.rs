//! EC2 round-trip E2E for image attributes and reserved-instance
//! listings/modifications — proving writes persist and reflect on read
//! (bug-hunt 2026-06-13 findings 1.2 and 1.5).

mod helpers;

use helpers::TestServer;

/// Buy `count` of a real one-year Standard m5.large offering in us-east-1a,
/// wait out its payment so it is active, and return the reservation id.
async fn purchase_ri(c: &aws_sdk_ec2::Client, count: i32) -> String {
    let offering_id = c
        .describe_reserved_instances_offerings()
        .instance_type(aws_sdk_ec2::types::InstanceType::M5Large)
        .availability_zone("us-east-1a")
        .max_duration(31_536_000)
        .send()
        .await
        .unwrap()
        .reserved_instances_offerings()[0]
        .reserved_instances_offering_id()
        .unwrap()
        .to_string();
    let id = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id(offering_id)
        .instance_count(count)
        .send()
        .await
        .unwrap()
        .reserved_instances_id()
        .unwrap()
        .to_string();
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
    id
}

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
    use aws_sdk_ec2::types::{Filter, ListingState, ListingStatus, PriceScheduleSpecification};
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let ri = purchase_ri(&c, 2).await;
    let schedule = |term: i64, price: f64| {
        PriceScheduleSpecification::builder()
            .term(term)
            .price(price)
            .currency_code(aws_sdk_ec2::types::CurrencyCodeValues::Usd)
            .build()
    };
    let create = |count: i32, token: &'static str| {
        c.create_reserved_instances_listing()
            .reserved_instances_id(&ri)
            .instance_count(count)
            .client_token(token)
            .price_schedules(schedule(11, 120.0))
            .price_schedules(schedule(4, 40.0))
            .send()
    };
    let created = create(1, "tok-1").await.unwrap();
    let listing = &created.reserved_instances_listings()[0];
    let listing_id = listing.reserved_instances_listing_id().unwrap().to_string();
    assert_eq!(listing.reserved_instances_id(), Some(ri.as_str()));
    assert_eq!(listing.status(), Some(&ListingStatus::Active));
    assert_eq!(listing.client_token(), Some("tok-1"));
    assert!(listing.create_date().is_some() && listing.update_date().is_some());
    let available = listing
        .instance_counts()
        .iter()
        .find(|ic| ic.state() == Some(&ListingState::Available))
        .unwrap();
    assert_eq!(available.instance_count(), Some(1));
    // A price per month left, the current one active.
    let schedules = listing.price_schedules();
    assert_eq!(schedules.len(), 11, "{schedules:?}");
    assert_eq!(schedules[0].term(), Some(11));
    assert_eq!(schedules[0].price(), Some(120.0));
    assert_eq!(schedules[0].active(), Some(true));
    assert_eq!(schedules[10].term(), Some(1));
    assert_eq!(schedules[10].price(), Some(40.0));

    // The same client token answers the same listing.
    let again = create(1, "tok-1").await.unwrap();
    assert_eq!(
        again.reserved_instances_listings()[0].reserved_instances_listing_id(),
        Some(listing_id.as_str())
    );
    // Only one instance is left to list.
    let err = create(2, "tok-2").await.unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidParameterValue")
    );

    let listings = |filter: Option<Filter>| {
        let mut r = c.describe_reserved_instances_listings();
        if let Some(f) = filter {
            r = r.filters(f);
        }
        r.send()
    };
    let described = listings(None).await.unwrap();
    assert_eq!(described.reserved_instances_listings().len(), 1);
    let status = |v: &str| Filter::builder().name("status").values(v).build();
    assert_eq!(
        listings(Some(status("active")))
            .await
            .unwrap()
            .reserved_instances_listings()
            .len(),
        1
    );
    let filtered = c
        .describe_reserved_instances_listings()
        .reserved_instances_listing_id(&listing_id)
        .send()
        .await
        .unwrap();
    assert_eq!(filtered.reserved_instances_listings().len(), 1);
    let err = listings(Some(Filter::builder().name("bogus").values("x").build()))
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().message(),
        Some("The filter 'bogus' is invalid")
    );

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
        described.reserved_instances_listings()[0].status(),
        Some(&ListingStatus::Cancelled)
    );
    assert!(listings(Some(status("active")))
        .await
        .unwrap()
        .reserved_instances_listings()
        .is_empty());

    // An unknown listing is refused, as on EC2.
    let err = c
        .cancel_reserved_instances_listing()
        .reserved_instances_listing_id("e5a2ff3b-7d14-494f-90af-0b5d00000000")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(err.meta().code(), Some("InvalidInput"));
    assert_eq!(err.meta().message(), Some(" Invalid value for 'listingId'"));
}

#[tokio::test]
async fn reserved_instances_modifications_persist_and_describe() {
    use aws_sdk_ec2::types::{Filter, ReservedInstanceState, ReservedInstancesConfiguration};
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let ri = purchase_ri(&c, 2).await;

    // Two m5.large become one m5.xlarge in another zone: the same footprint.
    let modified = c
        .modify_reserved_instances()
        .reserved_instances_ids(&ri)
        .client_token("modtok")
        .target_configurations(
            ReservedInstancesConfiguration::builder()
                .instance_count(1)
                .instance_type(aws_sdk_ec2::types::InstanceType::M5Xlarge)
                .availability_zone("us-east-1b")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let mod_id = modified
        .reserved_instances_modification_id()
        .unwrap()
        .to_string();
    assert!(mod_id.starts_with("rimod-"), "{mod_id}");

    let describe = || async {
        c.describe_reserved_instances_modifications()
            .reserved_instances_modification_ids(&mod_id)
            .send()
            .await
            .unwrap()
            .reserved_instances_modifications()[0]
            .clone()
    };
    let processing = describe().await;
    assert_eq!(processing.status(), Some("processing"));
    assert_eq!(processing.client_token(), Some("modtok"));
    assert_eq!(
        processing.reserved_instances_ids()[0].reserved_instances_id(),
        Some(ri.as_str())
    );
    assert!(processing.modification_results()[0]
        .reserved_instances_id()
        .is_none());
    assert_eq!(
        c.describe_reserved_instances()
            .send()
            .await
            .unwrap()
            .reserved_instances()
            .len(),
        1
    );

    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
    let fulfilled = describe().await;
    assert_eq!(fulfilled.status(), Some("fulfilled"));
    let result = &fulfilled.modification_results()[0];
    let target = result.target_configuration().unwrap();
    assert_eq!(target.instance_count(), Some(1));
    assert_eq!(target.availability_zone(), Some("us-east-1b"));
    let made = result.reserved_instances_id().unwrap().to_string();

    let ris = |id: &str| {
        let id = id.to_string();
        let c = c.clone();
        async move {
            c.describe_reserved_instances()
                .reserved_instances_ids(id)
                .send()
                .await
                .unwrap()
                .reserved_instances()[0]
                .clone()
        }
    };
    let original = ris(&ri).await;
    assert_eq!(original.state(), Some(&ReservedInstanceState::Retired));
    let new = ris(&made).await;
    assert_eq!(new.state(), Some(&ReservedInstanceState::Active));
    assert_eq!(new.instance_type().map(|t| t.as_str()), Some("m5.xlarge"));
    assert_eq!(new.availability_zone(), Some("us-east-1b"));
    assert_eq!(new.end(), original.end());

    // Filters, as modelled.
    let by_status = c
        .describe_reserved_instances_modifications()
        .filters(Filter::builder().name("status").values("fulfilled").build())
        .send()
        .await
        .unwrap();
    assert_eq!(by_status.reserved_instances_modifications().len(), 1);
    let by_result = c
        .describe_reserved_instances_modifications()
        .filters(
            Filter::builder()
                .name("modification-result.reserved-instances-id")
                .values(&made)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(by_result.reserved_instances_modifications().len(), 1);

    // Retired, the reservation cannot be modified again; an unknown one is
    // refused as on EC2.
    for (id, code) in [
        (ri.as_str(), "InvalidInput"),
        ("e5a2ff3b-7d14-494f-90af-0b5d00000000", "InvalidInput"),
        ("ri-1", "InvalidParameterValue"),
    ] {
        let err = c
            .modify_reserved_instances()
            .reserved_instances_ids(id)
            .target_configurations(
                ReservedInstancesConfiguration::builder()
                    .instance_count(2)
                    .availability_zone("us-east-1c")
                    .build(),
            )
            .send()
            .await
            .unwrap_err()
            .into_service_error();
        assert_eq!(err.meta().code(), Some(code), "{id}: {err:?}");
    }
}
