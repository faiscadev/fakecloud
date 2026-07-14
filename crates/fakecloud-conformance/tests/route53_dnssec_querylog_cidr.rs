//! Route 53 batch 4 conformance: DNSSEC + KSK + Query Logging + CIDR.

mod helpers;

use aws_sdk_route53::types::{CidrCollectionChange, CidrCollectionChangeAction};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

async fn make_zone(server: &TestServer, name: &str, caller: &str) -> String {
    let r53 = server.route53_client().await;
    r53.create_hosted_zone()
        .name(name)
        .caller_reference(caller)
        .send()
        .await
        .unwrap()
        .hosted_zone()
        .unwrap()
        .id()
        .to_string()
}

async fn make_zone_and_ksk(prefix: &str) -> (TestServer, String, String) {
    let server = TestServer::start().await;
    let zone = make_zone(
        &server,
        &format!("{prefix}.example.com"),
        &format!("{prefix}-cr"),
    )
    .await;
    let r53 = server.route53_client().await;
    let name = format!("{prefix}_ksk");
    r53.create_key_signing_key()
        .caller_reference(format!("{prefix}-ksk"))
        .hosted_zone_id(&zone)
        .key_management_service_arn(format!(
            "arn:aws:kms:us-east-1:000000000000:key/{}-0000-0000-0000-000000000000",
            prefix
        ))
        .name(&name)
        .status("INACTIVE")
        .send()
        .await
        .unwrap();
    (server, zone, name)
}

async fn make_cidr_collection(prefix: &str) -> (TestServer, String) {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    let id = r53
        .create_cidr_collection()
        .name(format!("{prefix}-collection"))
        .caller_reference(format!("{prefix}-cr"))
        .send()
        .await
        .unwrap()
        .collection()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    (server, id)
}

#[test_action("route53", "GetDNSSEC", checksum = "efdcfe57")]
#[tokio::test]
async fn r53_get_dnssec() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "conf-dnssec.example.com", "conf-dnssec-1").await;
    let r53 = server.route53_client().await;
    r53.get_dnssec().hosted_zone_id(&zone).send().await.unwrap();
}

#[test_action("route53", "EnableHostedZoneDNSSEC", checksum = "273fad40")]
#[tokio::test]
async fn r53_enable_hosted_zone_dnssec() {
    let server = TestServer::start().await;
    let zone = make_zone(
        &server,
        "conf-enable-dnssec.example.com",
        "conf-enable-dnssec-1",
    )
    .await;
    let r53 = server.route53_client().await;
    // AWS refuses to enable signing until the zone has an ACTIVE
    // key-signing key, so create one first.
    r53.create_key_signing_key()
        .caller_reference("conf-enable-dnssec-ksk")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/enabledns-0000-0000-0000-000000000000",
        )
        .name("conf_enable_dnssec_ksk")
        .status("ACTIVE")
        .send()
        .await
        .unwrap();
    r53.enable_hosted_zone_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "DisableHostedZoneDNSSEC", checksum = "2729707b")]
#[tokio::test]
async fn r53_disable_hosted_zone_dnssec() {
    let server = TestServer::start().await;
    let zone = make_zone(
        &server,
        "conf-disable-dnssec.example.com",
        "conf-disable-dnssec-1",
    )
    .await;
    let r53 = server.route53_client().await;
    r53.disable_hosted_zone_dnssec()
        .hosted_zone_id(&zone)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "CreateKeySigningKey", checksum = "342eca6f")]
#[tokio::test]
async fn r53_create_key_signing_key() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "conf-cks.example.com", "conf-cks-1").await;
    let r53 = server.route53_client().await;
    r53.create_key_signing_key()
        .caller_reference("conf-cks-key")
        .hosted_zone_id(&zone)
        .key_management_service_arn(
            "arn:aws:kms:us-east-1:000000000000:key/00000000-1111-2222-3333-444444444444",
        )
        .name("conf_cks_ksk")
        .status("INACTIVE")
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "ActivateKeySigningKey", checksum = "02e48392")]
#[tokio::test]
async fn r53_activate_key_signing_key() {
    let (server, zone, name) = make_zone_and_ksk("conf-activate").await;
    let r53 = server.route53_client().await;
    r53.activate_key_signing_key()
        .hosted_zone_id(&zone)
        .name(&name)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "DeactivateKeySigningKey", checksum = "7d19a64b")]
#[tokio::test]
async fn r53_deactivate_key_signing_key() {
    let (server, zone, name) = make_zone_and_ksk("conf-deactivate").await;
    let r53 = server.route53_client().await;
    r53.activate_key_signing_key()
        .hosted_zone_id(&zone)
        .name(&name)
        .send()
        .await
        .unwrap();
    r53.deactivate_key_signing_key()
        .hosted_zone_id(&zone)
        .name(&name)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "DeleteKeySigningKey", checksum = "5cada097")]
#[tokio::test]
async fn r53_delete_key_signing_key() {
    let (server, zone, name) = make_zone_and_ksk("conf-del-ksk").await;
    let r53 = server.route53_client().await;
    r53.delete_key_signing_key()
        .hosted_zone_id(&zone)
        .name(&name)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "CreateQueryLoggingConfig", checksum = "2220d281")]
#[tokio::test]
async fn r53_create_query_logging_config() {
    let server = TestServer::start().await;
    let zone = make_zone(
        &server,
        "conf-qlog-create.example.com",
        "conf-qlog-create-1",
    )
    .await;
    let r53 = server.route53_client().await;
    r53.create_query_logging_config()
        .hosted_zone_id(&zone)
        .cloud_watch_logs_log_group_arn(
            "arn:aws:logs:us-east-1:000000000000:log-group:/route53/conf-qlog",
        )
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "GetQueryLoggingConfig", checksum = "d1c3e4b5")]
#[tokio::test]
async fn r53_get_query_logging_config() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "conf-qlog-get.example.com", "conf-qlog-get-1").await;
    let r53 = server.route53_client().await;
    let id = r53
        .create_query_logging_config()
        .hosted_zone_id(&zone)
        .cloud_watch_logs_log_group_arn(
            "arn:aws:logs:us-east-1:000000000000:log-group:/route53/conf-qlog",
        )
        .send()
        .await
        .unwrap()
        .query_logging_config()
        .unwrap()
        .id()
        .to_string();
    r53.get_query_logging_config().id(&id).send().await.unwrap();
}

#[test_action("route53", "DeleteQueryLoggingConfig", checksum = "f33bf8c8")]
#[tokio::test]
async fn r53_delete_query_logging_config() {
    let server = TestServer::start().await;
    let zone = make_zone(&server, "conf-qlog-del.example.com", "conf-qlog-del-1").await;
    let r53 = server.route53_client().await;
    let id = r53
        .create_query_logging_config()
        .hosted_zone_id(&zone)
        .cloud_watch_logs_log_group_arn(
            "arn:aws:logs:us-east-1:000000000000:log-group:/route53/conf-qlog",
        )
        .send()
        .await
        .unwrap()
        .query_logging_config()
        .unwrap()
        .id()
        .to_string();
    r53.delete_query_logging_config()
        .id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "ListQueryLoggingConfigs", checksum = "22508d87")]
#[tokio::test]
async fn r53_list_query_logging_configs() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    r53.list_query_logging_configs().send().await.unwrap();
}

#[test_action("route53", "CreateCidrCollection", checksum = "7dc38108")]
#[tokio::test]
async fn r53_create_cidr_collection() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    r53.create_cidr_collection()
        .name("conf-cidr-create")
        .caller_reference("conf-cidr-create-1")
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "ChangeCidrCollection", checksum = "bab21ca5")]
#[tokio::test]
async fn r53_change_cidr_collection() {
    let (server, id) = make_cidr_collection("conf-cidr-change").await;
    let r53 = server.route53_client().await;
    let put = CidrCollectionChange::builder()
        .location_name("us-east-1")
        .action(CidrCollectionChangeAction::Put)
        .cidr_list("10.0.0.0/24")
        .build()
        .unwrap();
    r53.change_cidr_collection()
        .id(&id)
        .changes(put)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "DeleteCidrCollection", checksum = "7ffe64d9")]
#[tokio::test]
async fn r53_delete_cidr_collection() {
    let (server, id) = make_cidr_collection("conf-cidr-del").await;
    let r53 = server.route53_client().await;
    r53.delete_cidr_collection().id(&id).send().await.unwrap();
}

#[test_action("route53", "ListCidrCollections", checksum = "13cfe003")]
#[tokio::test]
async fn r53_list_cidr_collections() {
    let server = TestServer::start().await;
    let r53 = server.route53_client().await;
    r53.list_cidr_collections().send().await.unwrap();
}

#[test_action("route53", "ListCidrLocations", checksum = "cd5a29cf")]
#[tokio::test]
async fn r53_list_cidr_locations() {
    let (server, id) = make_cidr_collection("conf-cidr-loc").await;
    let r53 = server.route53_client().await;
    r53.list_cidr_locations()
        .collection_id(&id)
        .send()
        .await
        .unwrap();
}

#[test_action("route53", "ListCidrBlocks", checksum = "bb926c83")]
#[tokio::test]
async fn r53_list_cidr_blocks() {
    let (server, id) = make_cidr_collection("conf-cidr-blocks").await;
    let r53 = server.route53_client().await;
    let put = CidrCollectionChange::builder()
        .location_name("us-west-2")
        .action(CidrCollectionChangeAction::Put)
        .cidr_list("203.0.113.0/24")
        .build()
        .unwrap();
    r53.change_cidr_collection()
        .id(&id)
        .changes(put)
        .send()
        .await
        .unwrap();
    r53.list_cidr_blocks()
        .collection_id(&id)
        .send()
        .await
        .unwrap();
}
