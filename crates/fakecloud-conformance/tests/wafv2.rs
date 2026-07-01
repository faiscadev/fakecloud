//! WAF v2 conformance tests.

mod helpers;

use aws_sdk_wafv2::types::{DefaultAction, IpAddressVersion, Platform, Scope, VisibilityConfig};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

fn vis(name: &str) -> VisibilityConfig {
    VisibilityConfig::builder()
        .sampled_requests_enabled(false)
        .cloud_watch_metrics_enabled(false)
        .metric_name(name)
        .build()
        .unwrap()
}

fn allow_default() -> DefaultAction {
    DefaultAction::builder()
        .allow(aws_sdk_wafv2::types::AllowAction::builder().build())
        .build()
}

async fn make_acl(server: &TestServer, name: &str) -> (String, String, String) {
    let waf = server.wafv2_client().await;
    let s = waf
        .create_web_acl()
        .name(name)
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis(name))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    (
        s.id().unwrap().to_owned(),
        s.arn().unwrap().to_owned(),
        s.lock_token().unwrap().to_owned(),
    )
}

async fn make_rule_group(server: &TestServer, name: &str) -> (String, String, String) {
    let waf = server.wafv2_client().await;
    let s = waf
        .create_rule_group()
        .name(name)
        .scope(Scope::Regional)
        .capacity(50)
        .visibility_config(vis(name))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    (
        s.id().unwrap().to_owned(),
        s.arn().unwrap().to_owned(),
        s.lock_token().unwrap().to_owned(),
    )
}

async fn make_ip_set(server: &TestServer, name: &str) -> (String, String, String) {
    let waf = server.wafv2_client().await;
    let s = waf
        .create_ip_set()
        .name(name)
        .scope(Scope::Regional)
        .ip_address_version(IpAddressVersion::Ipv4)
        .addresses("203.0.113.0/24")
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    (
        s.id().unwrap().to_owned(),
        s.arn().unwrap().to_owned(),
        s.lock_token().unwrap().to_owned(),
    )
}

async fn make_regex_set(server: &TestServer, name: &str) -> (String, String, String) {
    let waf = server.wafv2_client().await;
    let s = waf
        .create_regex_pattern_set()
        .name(name)
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    (
        s.id().unwrap().to_owned(),
        s.arn().unwrap().to_owned(),
        s.lock_token().unwrap().to_owned(),
    )
}

#[test_action("wafv2", "CreateWebACL", checksum = "43232eb5")]
#[tokio::test]
async fn waf_create_web_acl() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .create_web_acl()
        .name("conf-create")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("conf-create"))
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetWebACL", checksum = "587701da")]
#[tokio::test]
async fn waf_get_web_acl() {
    let server = TestServer::start().await;
    let (id, _, _) = make_acl(&server, "conf-get").await;
    server
        .wafv2_client()
        .await
        .get_web_acl()
        .name("conf-get")
        .scope(Scope::Regional)
        .id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListWebACLs", checksum = "1659a5a4")]
#[tokio::test]
async fn waf_list_web_acls() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_web_acls()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "UpdateWebACL", checksum = "43cbab40")]
#[tokio::test]
async fn waf_update_web_acl() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_acl(&server, "conf-update").await;
    server
        .wafv2_client()
        .await
        .update_web_acl()
        .name("conf-update")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .default_action(allow_default())
        .visibility_config(vis("conf-update"))
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteWebACL", checksum = "6c96fae9")]
#[tokio::test]
async fn waf_delete_web_acl() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_acl(&server, "conf-delete").await;
    server
        .wafv2_client()
        .await
        .delete_web_acl()
        .name("conf-delete")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "CreateRuleGroup", checksum = "38854299")]
#[tokio::test]
async fn waf_create_rule_group() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .create_rule_group()
        .name("conf-rg-create")
        .scope(Scope::Regional)
        .capacity(50)
        .visibility_config(vis("conf-rg-create"))
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetRuleGroup", checksum = "d48b472b")]
#[tokio::test]
async fn waf_get_rule_group() {
    let server = TestServer::start().await;
    let (id, _, _) = make_rule_group(&server, "conf-rg-get").await;
    server
        .wafv2_client()
        .await
        .get_rule_group()
        .name("conf-rg-get")
        .scope(Scope::Regional)
        .id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListRuleGroups", checksum = "1ca4a444")]
#[tokio::test]
async fn waf_list_rule_groups() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_rule_groups()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "UpdateRuleGroup", checksum = "2bf5e4fb")]
#[tokio::test]
async fn waf_update_rule_group() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_rule_group(&server, "conf-rg-update").await;
    server
        .wafv2_client()
        .await
        .update_rule_group()
        .name("conf-rg-update")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .visibility_config(vis("conf-rg-update"))
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteRuleGroup", checksum = "e8e3044c")]
#[tokio::test]
async fn waf_delete_rule_group() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_rule_group(&server, "conf-rg-del").await;
    server
        .wafv2_client()
        .await
        .delete_rule_group()
        .name("conf-rg-del")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "CreateIPSet", checksum = "d188659f")]
#[tokio::test]
async fn waf_create_ip_set() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .create_ip_set()
        .name("conf-ips-create")
        .scope(Scope::Regional)
        .ip_address_version(IpAddressVersion::Ipv4)
        .addresses("203.0.113.0/24")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetIPSet", checksum = "0e378ad0")]
#[tokio::test]
async fn waf_get_ip_set() {
    let server = TestServer::start().await;
    let (id, _, _) = make_ip_set(&server, "conf-ips-get").await;
    server
        .wafv2_client()
        .await
        .get_ip_set()
        .name("conf-ips-get")
        .scope(Scope::Regional)
        .id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListIPSets", checksum = "bdd8a7e6")]
#[tokio::test]
async fn waf_list_ip_sets() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_ip_sets()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "UpdateIPSet", checksum = "5ab8ad58")]
#[tokio::test]
async fn waf_update_ip_set() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_ip_set(&server, "conf-ips-upd").await;
    server
        .wafv2_client()
        .await
        .update_ip_set()
        .name("conf-ips-upd")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .addresses("203.0.113.0/24")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteIPSet", checksum = "e8d0103e")]
#[tokio::test]
async fn waf_delete_ip_set() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_ip_set(&server, "conf-ips-del").await;
    server
        .wafv2_client()
        .await
        .delete_ip_set()
        .name("conf-ips-del")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "CreateRegexPatternSet", checksum = "2981f28e")]
#[tokio::test]
async fn waf_create_regex_pattern_set() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .create_regex_pattern_set()
        .name("conf-rx-create")
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetRegexPatternSet", checksum = "49413f28")]
#[tokio::test]
async fn waf_get_regex_pattern_set() {
    let server = TestServer::start().await;
    let (id, _, _) = make_regex_set(&server, "conf-rx-get").await;
    server
        .wafv2_client()
        .await
        .get_regex_pattern_set()
        .name("conf-rx-get")
        .scope(Scope::Regional)
        .id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListRegexPatternSets", checksum = "1836f6e2")]
#[tokio::test]
async fn waf_list_regex_pattern_sets() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_regex_pattern_sets()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "UpdateRegexPatternSet", checksum = "852bb410")]
#[tokio::test]
async fn waf_update_regex_pattern_set() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_regex_set(&server, "conf-rx-upd").await;
    server
        .wafv2_client()
        .await
        .update_regex_pattern_set()
        .name("conf-rx-upd")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteRegexPatternSet", checksum = "d35b60f4")]
#[tokio::test]
async fn waf_delete_regex_pattern_set() {
    let server = TestServer::start().await;
    let (id, _, lock) = make_regex_set(&server, "conf-rx-del").await;
    server
        .wafv2_client()
        .await
        .delete_regex_pattern_set()
        .name("conf-rx-del")
        .scope(Scope::Regional)
        .id(id)
        .lock_token(lock)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "AssociateWebACL", checksum = "b8cc61fe")]
#[tokio::test]
async fn waf_associate_web_acl() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-assoc").await;
    server
        .wafv2_client()
        .await
        .associate_web_acl()
        .web_acl_arn(arn)
        .resource_arn("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/x/abc")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DisassociateWebACL", checksum = "8c83ee23")]
#[tokio::test]
async fn waf_disassociate_web_acl() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .disassociate_web_acl()
        .resource_arn("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/x/abc")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetWebACLForResource", checksum = "ee84a23c")]
#[tokio::test]
async fn waf_get_web_acl_for_resource() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .get_web_acl_for_resource()
        .resource_arn("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/x/abc")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListResourcesForWebACL", checksum = "9ca5a0ca")]
#[tokio::test]
async fn waf_list_resources_for_web_acl() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-listres").await;
    server
        .wafv2_client()
        .await
        .list_resources_for_web_acl()
        .web_acl_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "PutLoggingConfiguration", checksum = "1263abf9")]
#[tokio::test]
async fn waf_put_logging_configuration() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-log-put").await;
    let cfg = aws_sdk_wafv2::types::LoggingConfiguration::builder()
        .resource_arn(arn)
        .log_destination_configs("arn:aws:logs:us-east-1:123456789012:log-group:test:*")
        .build()
        .unwrap();
    server
        .wafv2_client()
        .await
        .put_logging_configuration()
        .logging_configuration(cfg)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetLoggingConfiguration", checksum = "d07e8a4f")]
#[tokio::test]
async fn waf_get_logging_configuration() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-log-get").await;
    let cfg = aws_sdk_wafv2::types::LoggingConfiguration::builder()
        .resource_arn(&arn)
        .log_destination_configs("arn:aws:logs:us-east-1:123456789012:log-group:test:*")
        .build()
        .unwrap();
    let waf = server.wafv2_client().await;
    waf.put_logging_configuration()
        .logging_configuration(cfg)
        .send()
        .await
        .unwrap();
    waf.get_logging_configuration()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteLoggingConfiguration", checksum = "024b64c4")]
#[tokio::test]
async fn waf_delete_logging_configuration() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-log-del").await;
    let cfg = aws_sdk_wafv2::types::LoggingConfiguration::builder()
        .resource_arn(&arn)
        .log_destination_configs("arn:aws:logs:us-east-1:123456789012:log-group:test:*")
        .build()
        .unwrap();
    let waf = server.wafv2_client().await;
    waf.put_logging_configuration()
        .logging_configuration(cfg)
        .send()
        .await
        .unwrap();
    waf.delete_logging_configuration()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListLoggingConfigurations", checksum = "5f04b431")]
#[tokio::test]
async fn waf_list_logging_configurations() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_logging_configurations()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "PutPermissionPolicy", checksum = "e0d7d7f9")]
#[tokio::test]
async fn waf_put_permission_policy() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_rule_group(&server, "conf-pol-put").await;
    server
        .wafv2_client()
        .await
        .put_permission_policy()
        .resource_arn(arn)
        .policy(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetPermissionPolicy", checksum = "27467c52")]
#[tokio::test]
async fn waf_get_permission_policy() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_rule_group(&server, "conf-pol-get").await;
    let waf = server.wafv2_client().await;
    waf.put_permission_policy()
        .resource_arn(&arn)
        .policy(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .unwrap();
    waf.get_permission_policy()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeletePermissionPolicy", checksum = "a4fa83ae")]
#[tokio::test]
async fn waf_delete_permission_policy() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_rule_group(&server, "conf-pol-del").await;
    server
        .wafv2_client()
        .await
        .delete_permission_policy()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "TagResource", checksum = "81f3fa19")]
#[tokio::test]
async fn waf_tag_resource() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-tag").await;
    server
        .wafv2_client()
        .await
        .tag_resource()
        .resource_arn(arn)
        .tags(
            aws_sdk_wafv2::types::Tag::builder()
                .key("k")
                .value("v")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "UntagResource", checksum = "f7360971")]
#[tokio::test]
async fn waf_untag_resource() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-untag").await;
    let waf = server.wafv2_client().await;
    waf.tag_resource()
        .resource_arn(&arn)
        .tags(
            aws_sdk_wafv2::types::Tag::builder()
                .key("k")
                .value("v")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    waf.untag_resource()
        .resource_arn(arn)
        .tag_keys("k")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListTagsForResource", checksum = "117b8ef9")]
#[tokio::test]
async fn waf_list_tags_for_resource() {
    let server = TestServer::start().await;
    let (_, arn, _) = make_acl(&server, "conf-listtags").await;
    server
        .wafv2_client()
        .await
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "CreateAPIKey", checksum = "e67a9d35")]
#[tokio::test]
async fn waf_create_api_key() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .create_api_key()
        .scope(Scope::Regional)
        .token_domains("api.example.com")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteAPIKey", checksum = "8386ffe9")]
#[tokio::test]
async fn waf_delete_api_key() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let key = waf
        .create_api_key()
        .scope(Scope::Regional)
        .token_domains("api.example.com")
        .send()
        .await
        .unwrap()
        .api_key()
        .unwrap()
        .to_owned();
    waf.delete_api_key()
        .scope(Scope::Regional)
        .api_key(key)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetDecryptedAPIKey", checksum = "196117ea")]
#[tokio::test]
async fn waf_get_decrypted_api_key() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let key = waf
        .create_api_key()
        .scope(Scope::Regional)
        .token_domains("api.example.com")
        .send()
        .await
        .unwrap()
        .api_key()
        .unwrap()
        .to_owned();
    waf.get_decrypted_api_key()
        .scope(Scope::Regional)
        .api_key(key)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListAPIKeys", checksum = "78b7765f")]
#[tokio::test]
async fn waf_list_api_keys() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_api_keys()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DescribeAllManagedProducts", checksum = "4135a2db")]
#[tokio::test]
async fn waf_describe_all_managed_products() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .describe_all_managed_products()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DescribeManagedProductsByVendor", checksum = "fb8c879c")]
#[tokio::test]
async fn waf_describe_managed_products_by_vendor() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .describe_managed_products_by_vendor()
        .vendor_name("AWS")
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DescribeManagedRuleGroup", checksum = "806b50e2")]
#[tokio::test]
async fn waf_describe_managed_rule_group() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .describe_managed_rule_group()
        .vendor_name("AWS")
        .name("AWSManagedRulesCommonRuleSet")
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetManagedRuleSet", checksum = "f89c433b")]
#[tokio::test]
async fn waf_get_managed_rule_set() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    // GetManagedRuleSet resolves a customer-managed rule set that was published
    // via PutManagedRuleSetVersions; publish it first (with a version so the
    // set carries published version detail) before reading it back.
    waf.put_managed_rule_set_versions()
        .name("MyManagedRuleSet")
        .id("mrs-1")
        .scope(Scope::Regional)
        .lock_token("any")
        .recommended_version("Version_1.0")
        .versions_to_publish(
            "Version_1.0",
            aws_sdk_wafv2::types::VersionToPublish::builder()
                .forecasted_lifetime(30)
                .build(),
        )
        .send()
        .await
        .unwrap();
    waf.get_managed_rule_set()
        .name("MyManagedRuleSet")
        .id("mrs-1")
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListAvailableManagedRuleGroups", checksum = "bb50b60f")]
#[tokio::test]
async fn waf_list_available_managed_rule_groups() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_available_managed_rule_groups()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "wafv2",
    "ListAvailableManagedRuleGroupVersions",
    checksum = "503d6feb"
)]
#[tokio::test]
async fn waf_list_available_managed_rule_group_versions() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_available_managed_rule_group_versions()
        .vendor_name("AWS")
        .name("AWSManagedRulesCommonRuleSet")
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListManagedRuleSets", checksum = "4f9075df")]
#[tokio::test]
async fn waf_list_managed_rule_sets() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_managed_rule_sets()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "PutManagedRuleSetVersions", checksum = "7d7f6ac1")]
#[tokio::test]
async fn waf_put_managed_rule_set_versions() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .put_managed_rule_set_versions()
        .name("Set")
        .id("mrs-1")
        .scope(Scope::Regional)
        .lock_token("any")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "wafv2",
    "UpdateManagedRuleSetVersionExpiryDate",
    checksum = "79443e36"
)]
#[tokio::test]
async fn waf_update_managed_rule_set_version_expiry_date() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .update_managed_rule_set_version_expiry_date()
        .name("Set")
        .id("mrs-1")
        .scope(Scope::Regional)
        .lock_token("any")
        .version_to_expire("Version_1.0")
        .expiry_timestamp(aws_sdk_wafv2::primitives::DateTime::from_secs(
            1_700_000_000,
        ))
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GenerateMobileSdkReleaseUrl", checksum = "b8040e00")]
#[tokio::test]
async fn waf_generate_mobile_sdk_release_url() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .generate_mobile_sdk_release_url()
        .platform(Platform::Ios)
        .release_version("1.0.0")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetMobileSdkRelease", checksum = "a1a9a714")]
#[tokio::test]
async fn waf_get_mobile_sdk_release() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .get_mobile_sdk_release()
        .platform(Platform::Ios)
        .release_version("1.0.0")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "ListMobileSdkReleases", checksum = "697f62ea")]
#[tokio::test]
async fn waf_list_mobile_sdk_releases() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .list_mobile_sdk_releases()
        .platform(Platform::Ios)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "CheckCapacity", checksum = "b7f86716")]
#[tokio::test]
async fn waf_check_capacity() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .check_capacity()
        .scope(Scope::Regional)
        .rules(
            aws_sdk_wafv2::types::Rule::builder()
                .name("Rule1")
                .priority(0)
                .statement(
                    aws_sdk_wafv2::types::Statement::builder()
                        .geo_match_statement(
                            aws_sdk_wafv2::types::GeoMatchStatement::builder()
                                .country_codes(aws_sdk_wafv2::types::CountryCode::Us)
                                .build(),
                        )
                        .build(),
                )
                .action(
                    aws_sdk_wafv2::types::RuleAction::builder()
                        .allow(aws_sdk_wafv2::types::AllowAction::builder().build())
                        .build(),
                )
                .visibility_config(vis("Rule1"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetSampledRequests", checksum = "b0b0052d")]
#[tokio::test]
async fn waf_get_sampled_requests() {
    let server = TestServer::start().await;
    let now = aws_sdk_wafv2::primitives::DateTime::from_secs(1_700_000_000);
    let later = aws_sdk_wafv2::primitives::DateTime::from_secs(1_700_000_000 + 3600);
    server
        .wafv2_client()
        .await
        .get_sampled_requests()
        .web_acl_arn("arn:aws:wafv2:us-east-1:123456789012:regional/webacl/x/00000000")
        .rule_metric_name("metric")
        .scope(Scope::Regional)
        .time_window(
            aws_sdk_wafv2::types::TimeWindow::builder()
                .start_time(now)
                .end_time(later)
                .build()
                .unwrap(),
        )
        .max_items(10)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetTopPathStatisticsByTraffic", checksum = "e7d662bf")]
#[tokio::test]
async fn waf_get_top_path_statistics_by_traffic() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .get_top_path_statistics_by_traffic()
        .web_acl_arn("arn:aws:wafv2:us-east-1:123456789012:regional/webacl/x/00000000")
        .scope(Scope::Regional)
        .time_window(
            aws_sdk_wafv2::types::TimeWindow::builder()
                .start_time(aws_sdk_wafv2::primitives::DateTime::from_secs(
                    1_700_000_000,
                ))
                .end_time(aws_sdk_wafv2::primitives::DateTime::from_secs(
                    1_700_000_000 + 3600,
                ))
                .build()
                .unwrap(),
        )
        .limit(10)
        .number_of_top_traffic_bots_per_path(5)
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "GetRateBasedStatementManagedKeys", checksum = "6bee916c")]
#[tokio::test]
async fn waf_get_rate_based_statement_managed_keys() {
    let server = TestServer::start().await;
    server
        .wafv2_client()
        .await
        .get_rate_based_statement_managed_keys()
        .scope(Scope::Regional)
        .web_acl_name("acl")
        .web_acl_id("acl-id")
        .rule_name("rule")
        .send()
        .await
        .unwrap();
}

#[test_action("wafv2", "DeleteFirewallManagerRuleGroups", checksum = "9a7f466f")]
#[tokio::test]
async fn waf_delete_firewall_manager_rule_groups() {
    let server = TestServer::start().await;
    let (_, arn, lock) = make_acl(&server, "conf-fmd").await;
    server
        .wafv2_client()
        .await
        .delete_firewall_manager_rule_groups()
        .web_acl_arn(arn)
        .web_acl_lock_token(lock)
        .send()
        .await
        .unwrap();
}
