//! Route 53 E2E tests against the AWS Rust SDK.

mod helpers;

use aws_sdk_route53::types::{
    AliasTarget, Change, ChangeAction, ChangeBatch, GeoLocation, HostedZoneConfig, ResourceRecord,
    ResourceRecordSet, ResourceRecordSetFailover, ResourceRecordSetRegion, RrType,
};
use helpers::TestServer;

#[tokio::test]
async fn create_get_delete_hosted_zone_lifecycle() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_hosted_zone()
        .name("example.com")
        .caller_reference("e2e-1")
        .hosted_zone_config(
            HostedZoneConfig::builder()
                .comment("e2e zone")
                .private_zone(false)
                .build(),
        )
        .send()
        .await
        .expect("create hosted zone");
    let zone = create.hosted_zone().expect("zone");
    let id = zone.id().to_string();
    assert!(id.starts_with("/hostedzone/"));
    let name = zone.name();
    assert_eq!(name, "example.com.");
    let dset = create.delegation_set().expect("delegation set");
    assert_eq!(dset.name_servers().len(), 4);

    let got = r53
        .get_hosted_zone()
        .id(&id)
        .send()
        .await
        .expect("get hosted zone");
    assert_eq!(got.hosted_zone().unwrap().id(), id);
    assert_eq!(
        got.hosted_zone().unwrap().resource_record_set_count(),
        Some(2)
    );

    let count = r53.get_hosted_zone_count().send().await.expect("count");
    assert!(count.hosted_zone_count() >= 1);

    let list = r53.list_hosted_zones().send().await.expect("list");
    assert!(!list.hosted_zones().is_empty());

    r53.delete_hosted_zone()
        .id(&id)
        .send()
        .await
        .expect("delete");
}

#[tokio::test]
async fn change_and_list_resource_record_sets() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_hosted_zone()
        .name("rrset.example.com")
        .caller_reference("e2e-rrset")
        .send()
        .await
        .expect("create");
    let zone_id = create.hosted_zone().unwrap().id().to_string();

    let rrset = ResourceRecordSet::builder()
        .name("api.rrset.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.1")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let change = Change::builder()
        .action(ChangeAction::Upsert)
        .resource_record_set(rrset)
        .build()
        .unwrap();
    let batch = ChangeBatch::builder()
        .changes(change)
        .comment("upsert api A")
        .build()
        .unwrap();

    let upsert = r53
        .change_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .change_batch(batch)
        .send()
        .await
        .expect("change RRsets");
    let change_info = upsert.change_info().expect("change info");
    let change_id = change_info.id().to_string();

    let list = r53
        .list_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .send()
        .await
        .expect("list RRsets");
    let names: Vec<&str> = list
        .resource_record_sets()
        .iter()
        .map(|r| r.name())
        .collect();
    assert!(names.contains(&"api.rrset.example.com."));

    // Real Route53 returns PENDING during the propagation window then
    // INSYNC. Poll up to 10 reads — fakecloud flips after a small
    // fixed read-count threshold.
    let mut status = String::new();
    for _ in 0..10 {
        let got = r53
            .get_change()
            .id(&change_id)
            .send()
            .await
            .expect("get change");
        status = got.change_info().unwrap().status().as_str().to_string();
        if status == "INSYNC" {
            break;
        }
    }
    assert_eq!(status, "INSYNC");
}

#[tokio::test]
async fn delete_rrset_via_change_batch() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_hosted_zone()
        .name("delete.example.com")
        .caller_reference("e2e-delete")
        .send()
        .await
        .expect("create");
    let zone_id = create.hosted_zone().unwrap().id().to_string();

    let rrset = ResourceRecordSet::builder()
        .name("temp.delete.example.com.")
        .r#type(RrType::Cname)
        .ttl(300)
        .resource_records(
            ResourceRecord::builder()
                .value("origin.example.com")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    r53.change_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .change_batch(
            ChangeBatch::builder()
                .changes(
                    Change::builder()
                        .action(ChangeAction::Create)
                        .resource_record_set(rrset.clone())
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create rr");

    r53.change_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .change_batch(
            ChangeBatch::builder()
                .changes(
                    Change::builder()
                        .action(ChangeAction::Delete)
                        .resource_record_set(rrset)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("delete rr");

    let list = r53
        .list_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .send()
        .await
        .expect("list");
    let names: Vec<&str> = list
        .resource_record_sets()
        .iter()
        .map(|r| r.name())
        .collect();
    assert!(!names.contains(&"temp.delete.example.com."));
}

#[tokio::test]
async fn change_batch_is_atomic_on_invalid_change() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_hosted_zone()
        .name("atomic.example.com")
        .caller_reference("e2e-atomic")
        .send()
        .await
        .expect("create");
    let zone_id = create.hosted_zone().unwrap().id().to_string();

    let valid = ResourceRecordSet::builder()
        .name("ok.atomic.example.com.")
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
    let invalid = ResourceRecordSet::builder()
        .name("missing.atomic.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.20")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let result = r53
        .change_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .change_batch(
            ChangeBatch::builder()
                .changes(
                    Change::builder()
                        .action(ChangeAction::Create)
                        .resource_record_set(valid)
                        .build()
                        .unwrap(),
                )
                .changes(
                    Change::builder()
                        .action(ChangeAction::Delete)
                        .resource_record_set(invalid)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(result.is_err(), "expected the batch to be rejected");

    // Atomicity: even though the first change was valid, the failure of
    // the second change must roll back the create so the zone still has
    // only its default SOA + NS records.
    let list = r53
        .list_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .send()
        .await
        .expect("list after rejected batch");
    let names: Vec<&str> = list
        .resource_record_sets()
        .iter()
        .map(|r| r.name())
        .collect();
    assert!(!names.contains(&"ok.atomic.example.com."));
}

#[tokio::test]
async fn list_hosted_zones_by_name_works() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    r53.create_hosted_zone()
        .name("alpha.example.com")
        .caller_reference("byname-1")
        .send()
        .await
        .expect("create alpha");
    r53.create_hosted_zone()
        .name("beta.example.com")
        .caller_reference("byname-2")
        .send()
        .await
        .expect("create beta");

    let list = r53
        .list_hosted_zones_by_name()
        .send()
        .await
        .expect("list by name");
    assert!(list.hosted_zones().len() >= 2);
}

#[tokio::test]
async fn test_dns_answer_returns_record() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_hosted_zone()
        .name("dns.example.com")
        .caller_reference("e2e-dns")
        .send()
        .await
        .expect("create");
    let zone_id = create.hosted_zone().unwrap().id().to_string();
    // Strip /hostedzone/ prefix for the testdnsanswer endpoint.
    let zone_id_short = zone_id.trim_start_matches("/hostedzone/").to_string();

    let rrset = ResourceRecordSet::builder()
        .name("www.dns.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.42")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    r53.change_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .change_batch(
            ChangeBatch::builder()
                .changes(
                    Change::builder()
                        .action(ChangeAction::Upsert)
                        .resource_record_set(rrset)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("upsert");

    let answer = r53
        .test_dns_answer()
        .hosted_zone_id(zone_id_short)
        .record_name("www.dns.example.com")
        .record_type(RrType::A)
        .send()
        .await
        .expect("test dns");
    let data: Vec<&str> = answer.record_data().iter().map(|s| s.as_str()).collect();
    assert!(data.contains(&"203.0.113.42"));
}

/// Helper used by the routing-policy E2E tests below: create a zone,
/// upsert the supplied record sets, and return the unprefixed zone id
/// suitable for `TestDNSAnswer`.
async fn upsert_records_in_new_zone(
    r53: &aws_sdk_route53::Client,
    zone_name: &str,
    caller_ref: &str,
    records: Vec<ResourceRecordSet>,
) -> String {
    let create = r53
        .create_hosted_zone()
        .name(zone_name)
        .caller_reference(caller_ref)
        .send()
        .await
        .expect("create");
    let zone_id = create.hosted_zone().unwrap().id().to_string();
    let zone_id_short = zone_id.trim_start_matches("/hostedzone/").to_string();
    let mut batch = ChangeBatch::builder();
    for rrset in records {
        batch = batch.changes(
            Change::builder()
                .action(ChangeAction::Upsert)
                .resource_record_set(rrset)
                .build()
                .unwrap(),
        );
    }
    r53.change_resource_record_sets()
        .hosted_zone_id(&zone_id)
        .change_batch(batch.build().unwrap())
        .send()
        .await
        .expect("upsert");
    zone_id_short
}

#[tokio::test]
async fn test_dns_answer_weighted_routing_picks_record_by_subnet() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    let a = ResourceRecordSet::builder()
        .name("api.weighted.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("a")
        .weight(10)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.10")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let b = ResourceRecordSet::builder()
        .name("api.weighted.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("b")
        .weight(90)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.90")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let zid =
        upsert_records_in_new_zone(&r53, "weighted.example.com", "e2e-weighted", vec![a, b]).await;

    // Same subnet returns a deterministic answer, and that answer must
    // be one of the configured weighted records.
    let one = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("api.weighted.example.com")
        .record_type(RrType::A)
        .edns0_client_subnet_ip("203.0.113.5")
        .send()
        .await
        .expect("test dns weighted");
    let two = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("api.weighted.example.com")
        .record_type(RrType::A)
        .edns0_client_subnet_ip("203.0.113.5")
        .send()
        .await
        .expect("test dns weighted (repeat)");
    assert_eq!(one.record_data(), two.record_data());
    assert_eq!(one.record_data().len(), 1);
    assert!(
        ["203.0.113.10", "203.0.113.90"].contains(&one.record_data()[0].as_str()),
        "expected one of the weighted records, got {:?}",
        one.record_data()
    );
}

#[tokio::test]
async fn test_dns_answer_failover_serves_secondary_when_primary_unhealthy() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    // Failover requires HealthCheckId on the primary; create one and
    // flip it to Failure via the fakecloud admin endpoint.
    let hc_id = r53
        .create_health_check()
        .caller_reference("e2e-failover-hc")
        .health_check_config(
            aws_sdk_route53::types::HealthCheckConfig::builder()
                .r#type(aws_sdk_route53::types::HealthCheckType::Http)
                .ip_address("203.0.113.99")
                .port(80)
                .resource_path("/")
                .request_interval(30)
                .failure_threshold(3)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create hc")
        .health_check()
        .unwrap()
        .id()
        .to_string();
    let url = format!(
        "{}/_fakecloud/route53/health-checks/{}/status",
        server.endpoint(),
        hc_id
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({"status": "Failure", "reason": "Endpoint timed out"}))
        .send()
        .await
        .expect("flip hc to failure");
    assert!(resp.status().is_success(), "flip hc -> failure");

    let primary = ResourceRecordSet::builder()
        .name("api.failover.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("primary")
        .failover(ResourceRecordSetFailover::Primary)
        .health_check_id(&hc_id)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.10")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let secondary = ResourceRecordSet::builder()
        .name("api.failover.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("secondary")
        .failover(ResourceRecordSetFailover::Secondary)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.20")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let zid = upsert_records_in_new_zone(
        &r53,
        "failover.example.com",
        "e2e-failover",
        vec![primary, secondary],
    )
    .await;

    let answer = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("api.failover.example.com")
        .record_type(RrType::A)
        .send()
        .await
        .expect("test dns failover");
    let data: Vec<&str> = answer.record_data().iter().map(|s| s.as_str()).collect();
    assert_eq!(
        data,
        vec!["203.0.113.20"],
        "primary unhealthy -> answer must be the secondary record"
    );
}

#[tokio::test]
async fn test_dns_answer_geolocation_matches_country_record() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    let us = ResourceRecordSet::builder()
        .name("www.geo.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("us")
        .geo_location(GeoLocation::builder().country_code("US").build())
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.30")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let default = ResourceRecordSet::builder()
        .name("www.geo.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("default")
        .geo_location(GeoLocation::builder().country_code("*").build())
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.40")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let zid =
        upsert_records_in_new_zone(&r53, "geo.example.com", "e2e-geo", vec![us, default]).await;

    // Subnet first-octet 0..=63 maps to US in the deterministic
    // mapping used by the fakecloud resolver.
    let answer_us = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("www.geo.example.com")
        .record_type(RrType::A)
        .edns0_client_subnet_ip("10.0.0.1")
        .send()
        .await
        .expect("test dns geo us");
    let data_us: Vec<&str> = answer_us.record_data().iter().map(|s| s.as_str()).collect();
    assert_eq!(data_us, vec!["203.0.113.30"]);

    // 192..=223 -> SG / AS — no exact country, no continent match
    // -> falls through to the `*` default record.
    let answer_default = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("www.geo.example.com")
        .record_type(RrType::A)
        .edns0_client_subnet_ip("200.0.0.1")
        .send()
        .await
        .expect("test dns geo default");
    let data_default: Vec<&str> = answer_default
        .record_data()
        .iter()
        .map(|s| s.as_str())
        .collect();
    assert_eq!(data_default, vec!["203.0.113.40"]);
}

#[tokio::test]
async fn test_dns_answer_latency_picks_record_for_inferred_region() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    let east = ResourceRecordSet::builder()
        .name("api.latency.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("east")
        .region(ResourceRecordSetRegion::UsEast1)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.50")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let euw = ResourceRecordSet::builder()
        .name("api.latency.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .set_identifier("euw")
        .region(ResourceRecordSetRegion::EuWest1)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.60")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let zid =
        upsert_records_in_new_zone(&r53, "latency.example.com", "e2e-latency", vec![east, euw])
            .await;

    // 0..=63 -> us-east-1.
    let east_answer = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("api.latency.example.com")
        .record_type(RrType::A)
        .edns0_client_subnet_ip("10.0.0.1")
        .send()
        .await
        .expect("test dns latency east");
    let data_e: Vec<&str> = east_answer
        .record_data()
        .iter()
        .map(|s| s.as_str())
        .collect();
    assert_eq!(data_e, vec!["203.0.113.50"]);

    // 128..=159 -> eu-west-1.
    let eu_answer = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("api.latency.example.com")
        .record_type(RrType::A)
        .edns0_client_subnet_ip("130.0.0.1")
        .send()
        .await
        .expect("test dns latency eu");
    let data_w: Vec<&str> = eu_answer.record_data().iter().map(|s| s.as_str()).collect();
    assert_eq!(data_w, vec!["203.0.113.60"]);
}

#[tokio::test]
async fn test_dns_answer_multivalue_returns_all_healthy_up_to_eight() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    let mut sets: Vec<ResourceRecordSet> = Vec::new();
    for (i, ip) in ["203.0.113.1", "203.0.113.2", "203.0.113.3", "203.0.113.4"]
        .iter()
        .enumerate()
    {
        sets.push(
            ResourceRecordSet::builder()
                .name("api.mv.example.com.")
                .r#type(RrType::A)
                .ttl(60)
                .set_identifier(format!("rr-{i}"))
                .multi_value_answer(true)
                .resource_records(ResourceRecord::builder().value(*ip).build().unwrap())
                .build()
                .unwrap(),
        );
    }
    let zid = upsert_records_in_new_zone(&r53, "mv.example.com", "e2e-mv", sets).await;

    let answer = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("api.mv.example.com")
        .record_type(RrType::A)
        .send()
        .await
        .expect("test dns multivalue");
    let mut data: Vec<String> = answer.record_data().to_vec();
    data.sort();
    assert_eq!(
        data,
        vec![
            "203.0.113.1".to_string(),
            "203.0.113.2".to_string(),
            "203.0.113.3".to_string(),
            "203.0.113.4".to_string(),
        ]
    );
}

#[tokio::test]
async fn test_dns_answer_alias_target_resolves_against_elb_state() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let r53 = server.route53_client().await;

    // Create a real ALB so the alias DNS name resolves through the
    // ELBv2 cross-call lookup wired into Route53Service.
    let lb = elbv2
        .create_load_balancer()
        .name("alias-target-lb")
        .r#type(aws_sdk_elasticloadbalancingv2::types::LoadBalancerTypeEnum::Application)
        .send()
        .await
        .expect("create lb");
    let lb_obj = lb.load_balancers().first().expect("lb");
    let lb_dns = lb_obj.dns_name().unwrap().to_string();
    let lb_zone = lb_obj.canonical_hosted_zone_id().unwrap().to_string();

    let alias = ResourceRecordSet::builder()
        .name("www.alias.example.com.")
        .r#type(RrType::A)
        .alias_target(
            AliasTarget::builder()
                .hosted_zone_id(lb_zone)
                .dns_name(lb_dns)
                .evaluate_target_health(false)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let zid = upsert_records_in_new_zone(&r53, "alias.example.com", "e2e-alias", vec![alias]).await;

    let answer = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("www.alias.example.com")
        .record_type(RrType::A)
        .send()
        .await
        .expect("test dns alias");
    let data: Vec<String> = answer.record_data().to_vec();
    assert_eq!(data.len(), 1, "alias must resolve to exactly one A record");
    let v = &data[0];
    // If the LB has concrete addresses recorded those win; otherwise
    // the cross-call falls back to a documentation-range synthetic IP
    // (198.51.x.x). Either path is acceptable evidence that alias
    // resolution ran — and *both* paths require the lookup to succeed.
    assert!(
        v.starts_with("198.51.") || v.parse::<std::net::Ipv4Addr>().is_ok(),
        "expected an IPv4 alias answer, got {v}"
    );
}

#[tokio::test]
async fn test_dns_answer_alias_target_resolves_against_s3_bucket_state() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let r53 = server.route53_client().await;
    let bucket = "alias-target-bucket";
    s3.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create bucket");
    // S3 website endpoint pattern; the alias resolver looks up the
    // bucket name component against the wired S3 state and returns a
    // synthetic A record only when the bucket exists.
    let dns_name = format!("{bucket}.s3-website-us-east-1.amazonaws.com");
    let alias = ResourceRecordSet::builder()
        .name("static.alias-s3.example.com.")
        .r#type(RrType::A)
        .alias_target(
            AliasTarget::builder()
                .hosted_zone_id("Z3AQBSTGFYJSTF")
                .dns_name(dns_name)
                .evaluate_target_health(false)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let zid =
        upsert_records_in_new_zone(&r53, "alias-s3.example.com", "e2e-alias-s3", vec![alias]).await;

    let answer = r53
        .test_dns_answer()
        .hosted_zone_id(&zid)
        .record_name("static.alias-s3.example.com")
        .record_type(RrType::A)
        .send()
        .await
        .expect("test dns alias s3");
    assert_eq!(answer.record_data().len(), 1);
    assert!(answer.record_data()[0]
        .parse::<std::net::Ipv4Addr>()
        .is_ok());
}
