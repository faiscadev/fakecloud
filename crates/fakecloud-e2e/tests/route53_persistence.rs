mod helpers;

use aws_sdk_route53::types::{
    Change, ChangeAction, ChangeBatch, ResourceRecord, ResourceRecordSet, RrType,
};
use helpers::TestServer;

const POLICY_DOC: &str = r#"{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{"main":{"Type":"value","Value":"203.0.113.10"}},"StartEndpoint":"main"}"#;

/// Hosted zones, record sets, and traffic policies (the latter keyed by a
/// (id, version) tuple) all survive a restart in persistent mode.
#[tokio::test]
async fn persistence_round_trip_zone_records_traffic_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let r53 = server.route53_client().await;

    let zone = r53
        .create_hosted_zone()
        .name("persist.example.com")
        .caller_reference("persist-zone-1")
        .send()
        .await
        .unwrap()
        .hosted_zone
        .unwrap()
        .id;

    let rrset = ResourceRecordSet::builder()
        .name("www.persist.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.10")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    r53.change_resource_record_sets()
        .hosted_zone_id(&zone)
        .change_batch(
            ChangeBatch::builder()
                .changes(
                    Change::builder()
                        .action(ChangeAction::Create)
                        .resource_record_set(rrset)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let policy_id = r53
        .create_traffic_policy()
        .name("persist-policy")
        .document(POLICY_DOC)
        .send()
        .await
        .unwrap()
        .traffic_policy
        .unwrap()
        .id;

    server.restart().await;
    let r53 = server.route53_client().await;

    // Zone survives.
    assert_eq!(
        r53.get_hosted_zone()
            .id(&zone)
            .send()
            .await
            .unwrap()
            .hosted_zone()
            .unwrap()
            .name(),
        "persist.example.com."
    );

    // Record set survives.
    let rrsets = r53
        .list_resource_record_sets()
        .hosted_zone_id(&zone)
        .send()
        .await
        .unwrap();
    assert!(rrsets
        .resource_record_sets()
        .iter()
        .any(|r| r.name() == "www.persist.example.com." && r.r#type() == &RrType::A));

    // Traffic policy (tuple-keyed) survives.
    let pol = r53
        .get_traffic_policy()
        .id(&policy_id)
        .version(1)
        .send()
        .await
        .unwrap();
    assert_eq!(pol.traffic_policy().unwrap().id(), policy_id);
}

/// A deleted hosted zone stays gone after restart.
#[tokio::test]
async fn persistence_delete_zone_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let r53 = server.route53_client().await;

    let zone = r53
        .create_hosted_zone()
        .name("ephemeral.example.com")
        .caller_reference("ephemeral-1")
        .send()
        .await
        .unwrap()
        .hosted_zone
        .unwrap()
        .id;
    r53.delete_hosted_zone().id(&zone).send().await.unwrap();

    server.restart().await;
    let r53 = server.route53_client().await;

    assert!(r53.get_hosted_zone().id(&zone).send().await.is_err());
}
