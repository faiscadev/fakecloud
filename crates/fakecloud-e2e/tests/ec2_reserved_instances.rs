//! Reserved Instances: an offering listed by DescribeReservedInstancesOfferings
//! is bought through PurchaseReservedInstancesOffering and reported by
//! DescribeReservedInstances as the reservation of that offering.

mod helpers;

use aws_sdk_ec2::types::{
    DeleteQueuedReservedInstancesErrorCode, InstanceType, OfferingClassType, OfferingTypeValues,
    ReservedInstanceLimitPrice, ReservedInstanceState, Scope, Tenancy,
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
    // Paid for a moment after the purchase, then active.
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
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

async fn region_offering(c: &aws_sdk_ec2::Client) -> String {
    c.describe_reserved_instances_offerings()
        .instance_type(InstanceType::M5Large)
        .offering_class(OfferingClassType::Convertible)
        .max_duration(31_536_000)
        .send()
        .await
        .unwrap()
        .reserved_instances_offerings()[0]
        .reserved_instances_offering_id()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn a_queued_purchase_waits_and_can_be_deleted() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let offering = region_offering(&c).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let at = aws_smithy_types::DateTime::from_secs(now + 86_400);
    let id = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id(&offering)
        .instance_count(1)
        .purchase_time(at)
        .send()
        .await
        .unwrap()
        .reserved_instances_id()
        .unwrap()
        .to_string();
    let state = |id: String| {
        let c = c.clone();
        async move {
            let r = c
                .describe_reserved_instances()
                .reserved_instances_ids(id)
                .send()
                .await
                .unwrap();
            let ri = r.reserved_instances()[0].clone();
            (ri.state().cloned(), ri.start().map(|t| t.secs()))
        }
    };
    assert_eq!(
        state(id.clone()).await,
        (Some(ReservedInstanceState::Queued), Some(now + 86_400))
    );

    let deleted = c
        .delete_queued_reserved_instances()
        .reserved_instances_ids(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        deleted.successful_queued_purchase_deletions()[0].reserved_instances_id(),
        Some(id.as_str())
    );
    assert!(deleted.failed_queued_purchase_deletions().is_empty());
    assert_eq!(
        state(id.clone()).await.0,
        Some(ReservedInstanceState::QueuedDeleted)
    );
    let again = c
        .delete_queued_reserved_instances()
        .reserved_instances_ids(&id)
        .send()
        .await
        .unwrap();
    assert!(again.successful_queued_purchase_deletions().is_empty());
    assert_eq!(
        again.failed_queued_purchase_deletions()[0]
            .error()
            .and_then(|e| e.code()),
        Some(&DeleteQueuedReservedInstancesErrorCode::ReservedInstancesNotInQueuedState)
    );

    // A time in the past is refused.
    let err = c
        .purchase_reserved_instances_offering()
        .reserved_instances_offering_id(&offering)
        .instance_count(1)
        .purchase_time(aws_smithy_types::DateTime::from_secs(now - 3_600))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidParameterValue")
    );
}
