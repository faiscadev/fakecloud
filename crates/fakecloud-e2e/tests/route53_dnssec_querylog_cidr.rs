//! Route 53 batch 4 E2E: DNSSEC + KSK + Query Logging + CIDR Collections.

mod helpers;

use aws_sdk_route53::types::{
    Change, ChangeAction, ChangeBatch, CidrCollectionChange, ResourceRecord, ResourceRecordSet,
    RrType,
};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use helpers::TestServer;

async fn make_zone(server: &TestServer, name: &str, caller: &str) -> String {
    let r53 = server.route53_client().await;
    r53.create_hosted_zone()
        .name(name)
        .caller_reference(caller)
        .send()
        .await
        .expect("zone")
        .hosted_zone()
        .unwrap()
        .id()
        .to_string()
}

#[tokio::test]
async fn dnssec_status_default_and_toggle() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "dnssec.example.com", "dnssec-zone-1").await;
    let r53 = server.route53_client().await;

    let initial = r53
        .get_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("get dnssec");
    assert_eq!(
        initial.status().unwrap().serve_signature(),
        Some("NOT_SIGNING")
    );

    // AWS requires an ACTIVE key-signing key before signing can be enabled.
    r53.create_key_signing_key()
        .caller_reference("dnssec-toggle-ksk")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/toggle00-0000-0000-0000-000000000000",
        )
        .name("dnssec_toggle_ksk")
        .status("ACTIVE")
        .send()
        .await
        .expect("create ksk");

    r53.enable_hosted_zone_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("enable");
    let after_enable = r53
        .get_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("get");
    assert_eq!(
        after_enable.status().unwrap().serve_signature(),
        Some("SIGNING")
    );

    r53.disable_hosted_zone_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("disable");
    let after_disable = r53
        .get_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("get");
    assert_eq!(
        after_disable.status().unwrap().serve_signature(),
        Some("NOT_SIGNING")
    );
}

#[tokio::test]
async fn ksk_lifecycle_and_status_transitions() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "ksk.example.com", "ksk-zone-1").await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_key_signing_key()
        .caller_reference("ksk-1")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/abcd1234-ee11-4422-bb33-aabbccddeeff",
        )
        .name("primary_ksk")
        .status("INACTIVE")
        .send()
        .await
        .expect("create ksk");
    let ksk = create.key_signing_key().expect("ksk");
    assert_eq!(ksk.name(), Some("primary_ksk"));
    assert_eq!(ksk.status(), Some("INACTIVE"));
    assert_eq!(ksk.flag(), 257);
    // The response carries the full DNSSEC material derived from the keypair.
    assert!(ksk.key_tag() > 0);
    let public_key = ksk.public_key().expect("public_key");
    assert!(!public_key.is_empty());
    let digest_value = ksk.digest_value().expect("digest_value");
    assert_eq!(digest_value.len(), 64);
    assert!(digest_value
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()));
    let dnskey_record = ksk.dnskey_record().expect("dnskey_record");
    assert_eq!(dnskey_record, format!("257 3 13 {public_key}"));
    let ds_record = ksk.ds_record().expect("ds_record");
    assert_eq!(
        ds_record,
        format!("{} 13 2 {}", ksk.key_tag(), digest_value)
    );

    // Cannot delete an ACTIVE KSK; flip to ACTIVE first to confirm guard.
    r53.activate_key_signing_key()
        .hosted_zone_id(&zone)
        .name("primary_ksk")
        .send()
        .await
        .expect("activate");
    let bad = r53
        .delete_key_signing_key()
        .hosted_zone_id(&zone)
        .name("primary_ksk")
        .send()
        .await;
    assert!(bad.is_err(), "expected ACTIVE KSK to block delete");

    r53.deactivate_key_signing_key()
        .hosted_zone_id(&zone)
        .name("primary_ksk")
        .send()
        .await
        .expect("deactivate");

    r53.delete_key_signing_key()
        .hosted_zone_id(&zone)
        .name("primary_ksk")
        .send()
        .await
        .expect("delete");

    let dnssec = r53
        .get_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("get dnssec");
    assert!(dnssec.key_signing_keys().is_empty());
}

#[tokio::test]
async fn duplicate_ksk_name_is_rejected() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "dup-ksk.example.com", "dup-ksk-zone").await;
    let r53 = server.route53_client().await;

    r53.create_key_signing_key()
        .caller_reference("dup-1")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000001",
        )
        .name("dup_ksk")
        .status("INACTIVE")
        .send()
        .await
        .expect("first");

    let dup = r53
        .create_key_signing_key()
        .caller_reference("dup-2")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000002",
        )
        .name("dup_ksk")
        .status("INACTIVE")
        .send()
        .await;
    assert!(dup.is_err(), "expected duplicate name to be rejected");
}

#[tokio::test]
async fn query_logging_lifecycle() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "qlog.example.com", "qlog-zone-1").await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_query_logging_config()
        .hosted_zone_id(&zone)
        .cloud_watch_logs_log_group_arn(
            "arn:aws:logs:us-east-1:000000000000:log-group:/route53/qlog",
        )
        .send()
        .await
        .expect("create");
    let id = create.query_logging_config().unwrap().id().to_string();

    let got = r53
        .get_query_logging_config()
        .id(&id)
        .send()
        .await
        .expect("get");
    assert_eq!(got.query_logging_config().unwrap().id(), id);

    let list = r53.list_query_logging_configs().send().await.expect("list");
    assert!(!list.query_logging_configs().is_empty());

    // Duplicate per zone is rejected.
    let dup = r53
        .create_query_logging_config()
        .hosted_zone_id(&zone)
        .cloud_watch_logs_log_group_arn(
            "arn:aws:logs:us-east-1:000000000000:log-group:/route53/qlog2",
        )
        .send()
        .await;
    assert!(dup.is_err(), "duplicate query logging config rejected");

    r53.delete_query_logging_config()
        .id(&id)
        .send()
        .await
        .expect("delete");
    let after = r53.get_query_logging_config().id(&id).send().await;
    assert!(after.is_err(), "expected NoSuchQueryLoggingConfig");
}

#[tokio::test]
async fn cidr_collection_lifecycle() {
    use aws_sdk_route53::types::CidrCollectionChangeAction;
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let create = r53
        .create_cidr_collection()
        .name("collection-lifecycle")
        .caller_reference("cidr-1")
        .send()
        .await
        .expect("create");
    let id = create.collection().unwrap().id().unwrap().to_string();
    assert_eq!(create.collection().unwrap().version(), Some(1));
    // Route 53 CIDR-collection ARNs are global and omit the account id:
    // `arn:aws:route53::<empty-account>:cidrcollection/<id>`. The Terraform
    // `aws_route53_cidr_collection` resource asserts this exact shape.
    let arn = create.collection().unwrap().arn().unwrap();
    assert_eq!(arn, format!("arn:aws:route53:::cidrcollection/{id}"));

    let put = CidrCollectionChange::builder()
        .location_name("us-east-1")
        .action(CidrCollectionChangeAction::Put)
        .cidr_list("10.0.0.0/24")
        .cidr_list("10.0.1.0/24")
        .build()
        .unwrap();
    r53.change_cidr_collection()
        .id(&id)
        .collection_version(1)
        .changes(put)
        .send()
        .await
        .expect("put cidrs");

    let locs = r53
        .list_cidr_locations()
        .collection_id(&id)
        .send()
        .await
        .expect("list locations");
    assert_eq!(locs.cidr_locations().len(), 1);

    let blocks = r53
        .list_cidr_blocks()
        .collection_id(&id)
        .location_name("us-east-1")
        .send()
        .await
        .expect("list blocks");
    assert_eq!(blocks.cidr_blocks().len(), 2);

    let del = CidrCollectionChange::builder()
        .location_name("us-east-1")
        .action(CidrCollectionChangeAction::DeleteIfExists)
        .cidr_list("10.0.0.0/24")
        .cidr_list("10.0.1.0/24")
        .build()
        .unwrap();
    r53.change_cidr_collection()
        .id(&id)
        .collection_version(2)
        .changes(del)
        .send()
        .await
        .expect("delete cidrs");

    r53.delete_cidr_collection()
        .id(&id)
        .send()
        .await
        .expect("delete collection");
}

#[tokio::test]
async fn cidr_collection_in_use_blocks_delete() {
    use aws_sdk_route53::types::CidrCollectionChangeAction;
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let id = r53
        .create_cidr_collection()
        .name("collection-inuse")
        .caller_reference("cidr-inuse")
        .send()
        .await
        .expect("create")
        .collection()
        .unwrap()
        .id()
        .unwrap()
        .to_string();

    let put = CidrCollectionChange::builder()
        .location_name("eu-west-1")
        .action(CidrCollectionChangeAction::Put)
        .cidr_list("192.0.2.0/24")
        .build()
        .unwrap();
    r53.change_cidr_collection()
        .id(&id)
        .changes(put)
        .send()
        .await
        .expect("put");

    let res = r53.delete_cidr_collection().id(&id).send().await;
    assert!(res.is_err(), "non-empty collection should block delete");
}

#[tokio::test]
async fn cidr_collection_version_mismatch_rejected() {
    use aws_sdk_route53::types::CidrCollectionChangeAction;
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;

    let id = r53
        .create_cidr_collection()
        .name("collection-version")
        .caller_reference("cidr-ver")
        .send()
        .await
        .expect("create")
        .collection()
        .unwrap()
        .id()
        .unwrap()
        .to_string();

    let change = CidrCollectionChange::builder()
        .location_name("ap-south-1")
        .action(CidrCollectionChangeAction::Put)
        .cidr_list("198.51.100.0/24")
        .build()
        .unwrap();
    let stale = r53
        .change_cidr_collection()
        .id(&id)
        .collection_version(99)
        .changes(change)
        .send()
        .await;
    assert!(stale.is_err(), "stale collection version rejected");
}

// ─── U3: GetChange transition + DNSSEC RRSIG + query log delivery ───

/// `ChangeResourceRecordSets` produces a change ID. The first
/// `GetChange` returns `PENDING`; once the propagation window elapses
/// (default 1s) the next call flips to `INSYNC`. Mirrors real Route
/// 53's eventual-consistency model.
#[tokio::test]
async fn get_change_transitions_pending_to_insync_over_time() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "transition.example.com", "transition-zone").await;
    let r53 = server.route53_client().await;

    let rrset = ResourceRecordSet::builder()
        .name("foo.transition.example.com.")
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
    let change = Change::builder()
        .action(ChangeAction::Create)
        .resource_record_set(rrset)
        .build()
        .unwrap();
    let batch = ChangeBatch::builder().changes(change).build().unwrap();
    let resp = r53
        .change_resource_record_sets()
        .hosted_zone_id(&zone)
        .change_batch(batch)
        .send()
        .await
        .expect("change rrsets");
    let change_id = resp
        .change_info()
        .unwrap()
        .id()
        .trim_start_matches("/change/")
        .to_string();
    assert_eq!(resp.change_info().unwrap().status().as_str(), "PENDING");

    // Immediate read still PENDING.
    let first = r53.get_change().id(&change_id).send().await.expect("get");
    assert_eq!(first.change_info().unwrap().status().as_str(), "PENDING");

    // Sleep past the propagation window (1s default), then re-read.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let later = r53
        .get_change()
        .id(&change_id)
        .send()
        .await
        .expect("get later");
    assert_eq!(
        later.change_info().unwrap().status().as_str(),
        "INSYNC",
        "change should have flipped to INSYNC after the propagation window"
    );
}

/// DNSSEC chain-of-trust: the admin endpoint surfaces the
/// deterministic DNSKEY public key + DS digest, and `RRSIG`
/// signatures returned by the sign endpoint validate against the
/// public key. Confirms we're actually signing — not just stuffing
/// random bytes into a `<Signature>` element.
#[tokio::test]
async fn dnssec_rrsig_signs_and_verifies_against_dnskey() {
    use p256::ecdsa::signature::Verifier;
    use p256::ecdsa::{Signature, VerifyingKey};

    let server = TestServer::start().await;
    let zone = make_zone(&server, "signed.example.com", "signed-zone").await;
    let r53 = server.route53_client().await;

    // Create an ACTIVE KSK first — AWS refuses to enable signing until the
    // zone has one — then enable DNSSEC.
    r53.create_key_signing_key()
        .caller_reference("ksk-rrsig")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        .name("rrsig_ksk")
        .status("ACTIVE")
        .send()
        .await
        .expect("create ksk");
    r53.enable_hosted_zone_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .expect("enable dnssec");

    // Fetch the public key + DS digest via the admin endpoint.
    let material = server
        .route53_dnssec_material(&zone)
        .await
        .expect("dnssec material");
    let dnskey_pk_b64 = material["dnskeyPublicKeyB64"].as_str().unwrap();
    let dnskey_pk = B64.decode(dnskey_pk_b64).expect("base64 dnskey");
    assert_eq!(
        dnskey_pk.len(),
        64,
        "ECDSAP256SHA256 public key is 64 bytes (X || Y)"
    );
    let ds_hex = material["dsDigestSha256Hex"].as_str().unwrap();
    assert_eq!(ds_hex.len(), 64);
    assert!(ds_hex.chars().all(|c| c.is_ascii_hexdigit()));
    let key_tag = material["keyTag"].as_u64().unwrap() as u16;
    assert_eq!(material["algorithm"].as_u64().unwrap(), 13);
    assert_eq!(material["flags"].as_u64().unwrap(), 257);

    // Sign a synthetic A RRset and verify the signature.
    let signed = server
        .route53_dnssec_sign(&zone, "host.signed.example.com.", "A", 300, &["192.0.2.10"])
        .await
        .expect("sign");
    let sig_b64 = signed["signatureB64"].as_str().unwrap();
    let sig_bytes = B64.decode(sig_b64).unwrap();
    assert_eq!(sig_bytes.len(), 64, "ECDSA-P256 r||s = 64 bytes");
    assert_eq!(signed["keyTag"].as_u64().unwrap(), key_tag as u64);
    assert_eq!(signed["algorithm"].as_u64().unwrap(), 13);
    assert_eq!(signed["type"].as_str().unwrap(), "A");

    // Reconstruct the signed bytes the server hashed (RRSIG RDATA
    // without signature, followed by canonical RRset bytes), then
    // verify with a standalone P256 VerifyingKey built from the
    // DNSKEY public key.
    let mut sec1 = Vec::with_capacity(65);
    sec1.push(0x04);
    sec1.extend_from_slice(&dnskey_pk);
    let verifying_key =
        VerifyingKey::from_sec1_bytes(&sec1).expect("verifying key from DNSKEY public bytes");

    let signed_data = build_rrsig_input(
        signed["type"].as_str().unwrap(),
        13,
        signed["labels"].as_u64().unwrap() as u8,
        signed["originalTtl"].as_u64().unwrap() as u32,
        signed["expiration"].as_u64().unwrap() as u32,
        signed["inception"].as_u64().unwrap() as u32,
        key_tag,
        signed["signerName"].as_str().unwrap(),
        "host.signed.example.com.",
        300,
        &["192.0.2.10"],
    );
    let signature = Signature::from_slice(&sig_bytes).expect("p256 sig");
    verifying_key
        .verify(&signed_data, &signature)
        .expect("RRSIG must verify against the DNSKEY public key");
}

/// `TestDNSAnswer` writes a query log record into the configured
/// CloudWatch Logs group whenever the zone has a query logging
/// config. Tests the cross-call path from `fakecloud-route53` into
/// `fakecloud-logs::ingest::append_events`.
#[tokio::test]
async fn test_dns_answer_writes_query_log_event() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "qlog-delivery.example.com", "qlog-delivery").await;
    let r53 = server.route53_client().await;
    let logs = server.logs_client().await;

    // Create an A record so TestDNSAnswer has something to return.
    let rrset = ResourceRecordSet::builder()
        .name("api.qlog-delivery.example.com.")
        .r#type(RrType::A)
        .ttl(60)
        .resource_records(
            ResourceRecord::builder()
                .value("203.0.113.7")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let change = Change::builder()
        .action(ChangeAction::Create)
        .resource_record_set(rrset)
        .build()
        .unwrap();
    let batch = ChangeBatch::builder().changes(change).build().unwrap();
    r53.change_resource_record_sets()
        .hosted_zone_id(&zone)
        .change_batch(batch)
        .send()
        .await
        .expect("change rrsets");

    let log_group_name = "/aws/route53/qlog-delivery";
    logs.create_log_group()
        .log_group_name(log_group_name)
        .send()
        .await
        .expect("create log group");

    // Use the testkit's default account (123456789012) so the
    // route53 service writes the query log into the same logs state
    // bucket the SDK client reads from.
    let log_group_arn = format!(
        "arn:aws:logs:us-east-1:123456789012:log-group:{}",
        log_group_name
    );
    r53.create_query_logging_config()
        .hosted_zone_id(&zone)
        .cloud_watch_logs_log_group_arn(&log_group_arn)
        .send()
        .await
        .expect("create query logging config");

    // Issue a DNS test query — should land in the log group.
    r53.test_dns_answer()
        .hosted_zone_id(&zone)
        .record_name("api.qlog-delivery.example.com")
        .record_type(RrType::A)
        .send()
        .await
        .expect("test dns answer");

    // Listing log streams should reveal one stream named with the
    // current date (`FAKECLOUD/YYYY/MM/DD`) carrying our event.
    let streams = logs
        .describe_log_streams()
        .log_group_name(log_group_name)
        .send()
        .await
        .expect("describe log streams");
    assert!(
        !streams.log_streams().is_empty(),
        "expected at least one log stream after TestDNSAnswer"
    );
    let stream_name = streams.log_streams()[0]
        .log_stream_name()
        .expect("stream name")
        .to_string();
    assert!(
        stream_name.starts_with("FAKECLOUD/"),
        "stream name should have FAKECLOUD/YYYY/MM/DD prefix, got {stream_name}"
    );

    let events = logs
        .get_log_events()
        .log_group_name(log_group_name)
        .log_stream_name(&stream_name)
        .send()
        .await
        .expect("get log events");
    let evs = events.events();
    assert!(!evs.is_empty(), "expected at least one log event");
    let msg = evs[0].message().expect("message");
    assert!(msg.starts_with("1.0 "), "log starts with version 1.0");
    assert!(msg.contains("qlog-delivery.example.com"));
    assert!(msg.contains(" A "));
    assert!(msg.contains(" NOERROR "));
}

/// Build the bytes the route53 service feeds into ECDSA signing, so
/// the e2e test can re-derive the same input and verify against the
/// public key. Mirrors `crate::dnssec::rrsig_signed_data` +
/// `canonical_rrset_bytes` without depending on the crate (which is
/// a server-side dependency we don't pull into the e2e test).
#[allow(clippy::too_many_arguments)]
fn build_rrsig_input(
    rtype_name: &str,
    algorithm: u8,
    labels: u8,
    original_ttl: u32,
    expiration: u32,
    inception: u32,
    key_tag: u16,
    signer_name: &str,
    owner_name: &str,
    ttl: u32,
    rdata_values: &[&str],
) -> Vec<u8> {
    let rtype_code: u16 = match rtype_name {
        "A" => 1,
        "AAAA" => 28,
        _ => panic!("unsupported in test helper: {rtype_name}"),
    };
    let class_in: u16 = 1;
    let mut canonical = Vec::new();
    let mut rdatas: Vec<Vec<u8>> = rdata_values
        .iter()
        .map(|v| match rtype_code {
            1 => {
                let mut bytes = [0u8; 4];
                for (i, p) in v.split('.').enumerate() {
                    bytes[i] = p.parse().unwrap();
                }
                bytes.to_vec()
            }
            28 => v.parse::<std::net::Ipv6Addr>().unwrap().octets().to_vec(),
            _ => unreachable!(),
        })
        .collect();
    rdatas.sort();
    for r in &rdatas {
        encode_dns_name(&mut canonical, owner_name);
        canonical.extend_from_slice(&rtype_code.to_be_bytes());
        canonical.extend_from_slice(&class_in.to_be_bytes());
        canonical.extend_from_slice(&ttl.to_be_bytes());
        canonical.extend_from_slice(&(r.len() as u16).to_be_bytes());
        canonical.extend_from_slice(r);
    }
    let mut out = Vec::new();
    out.extend_from_slice(&rtype_code.to_be_bytes());
    out.push(algorithm);
    out.push(labels);
    out.extend_from_slice(&original_ttl.to_be_bytes());
    out.extend_from_slice(&expiration.to_be_bytes());
    out.extend_from_slice(&inception.to_be_bytes());
    out.extend_from_slice(&key_tag.to_be_bytes());
    encode_dns_name(&mut out, signer_name);
    out.extend_from_slice(&canonical);
    out
}

fn encode_dns_name(out: &mut Vec<u8>, name: &str) {
    let trimmed = name.trim_end_matches('.');
    if trimmed.is_empty() {
        out.push(0);
        return;
    }
    for label in trimmed.split('.') {
        let bytes = label.as_bytes();
        out.push(bytes.len() as u8);
        for &b in bytes {
            out.push(b.to_ascii_lowercase());
        }
    }
    out.push(0);
}
