//! Reserved Instances: an offering listed by DescribeReservedInstancesOfferings
//! is bought through PurchaseReservedInstancesOffering and reported by
//! DescribeReservedInstances as the reservation of that offering.

mod helpers;

use aws_sdk_ec2::types::{
    InstanceType, OfferingClassType, OfferingTypeValues, ReservedInstanceLimitPrice,
    ReservedInstanceState, Scope, Tenancy,
};
use helpers::TestServer;

#[tokio::test]
async fn a_purchased_offering_becomes_an_active_reservation() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let offerings = c
        .describe_reserved_instances_offerings()
        .instance_type(InstanceType::M5Large)
        .availability_zone("us-east-1b")
        .offering_type(OfferingTypeValues::AllUpfront)
        .instance_tenancy(Tenancy::Dedicated)
        .min_duration(94_608_000)
        .send()
        .await
        .unwrap();
    let offerings = offerings.reserved_instances_offerings();
    assert_eq!(offerings.len(), 1, "{offerings:?}");
    let offering_id = offerings[0].reserved_instances_offering_id().unwrap();

    let id = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id(offering_id)
        .instance_count(2)
        .limit_price(ReservedInstanceLimitPrice::builder().amount(10.0).build())
        .send()
        .await
        .unwrap()
        .reserved_instances_id()
        .unwrap()
        .to_string();

    let describe = || async {
        c.describe_reserved_instances()
            .reserved_instances_ids(&id)
            .send()
            .await
            .unwrap()
            .reserved_instances()[0]
            .clone()
    };
    let ri = describe().await;
    assert_eq!(ri.state(), Some(&ReservedInstanceState::PaymentPending));
    assert_eq!(ri.instance_type(), Some(&InstanceType::M5Large));
    assert_eq!(ri.availability_zone(), Some("us-east-1b"));
    assert_eq!(ri.instance_count(), Some(2));
    assert_eq!(ri.duration(), Some(94_608_000));
    assert_eq!(ri.offering_class(), Some(&OfferingClassType::Standard));
    assert_eq!(ri.offering_type(), Some(&OfferingTypeValues::AllUpfront));
    assert_eq!(ri.instance_tenancy(), Some(&Tenancy::Dedicated));
    assert_eq!(ri.scope(), Some(&Scope::AvailabilityZone));
    let (start, end) = (ri.start().unwrap(), ri.end().unwrap());
    assert_eq!(end.secs() - start.secs(), 94_608_000);
    assert_eq!(
        describe().await.state(),
        Some(&ReservedInstanceState::Active)
    );
}

#[tokio::test]
async fn an_unknown_offering_or_a_low_limit_price_is_refused() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let err = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id("4b2293b4-5813-4cc8-9ce3-1957f0000000")
        .instance_count(1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidReservedInstancesOfferingId")
    );

    let offering_id = c
        .describe_reserved_instances_offerings()
        .instance_type(InstanceType::T3Micro)
        .send()
        .await
        .unwrap()
        .reserved_instances_offerings()[0]
        .reserved_instances_offering_id()
        .unwrap()
        .to_string();
    let err = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id(&offering_id)
        .instance_count(1)
        .limit_price(ReservedInstanceLimitPrice::builder().amount(-1.0).build())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("LimitPriceExceeded")
    );
    assert!(c
        .describe_reserved_instances()
        .send()
        .await
        .unwrap()
        .reserved_instances()
        .is_empty());
}
