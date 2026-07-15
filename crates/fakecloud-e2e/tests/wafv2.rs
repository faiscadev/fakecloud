//! WAF v2 service E2E.

mod helpers;

use aws_sdk_wafv2::types::{
    DefaultAction, IpAddressVersion, IpSetReferenceStatement, Platform, Rule, RuleAction, Scope,
    Statement, Tag, VisibilityConfig,
};
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

#[tokio::test]
async fn cloudfront_scope_arn_uses_us_east_1_region_segment() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let arn = waf
        .create_web_acl()
        .name("cf-acl")
        .scope(Scope::Cloudfront)
        .default_action(allow_default())
        .visibility_config(vis("cf-acl"))
        .send()
        .await
        .expect("create")
        .summary
        .expect("summary")
        .arn()
        .expect("arn")
        .to_owned();
    // Real AWS CLOUDFRONT-scope ARNs always use us-east-1 + global path.
    assert!(
        arn.starts_with("arn:aws:wafv2:us-east-1:"),
        "CLOUDFRONT ARN must contain us-east-1 region segment, got: {arn}"
    );
    assert!(
        arn.contains(":global/webacl/cf-acl/"),
        "unexpected ARN: {arn}"
    );
}

#[tokio::test]
async fn regional_scope_arn_uses_regional_path_segment() {
    // REGIONAL-scope ARNs use the literal `regional` resource-path prefix (not
    // the region name), e.g. arn:aws:wafv2:us-east-1:acct:regional/ipset/name/id.
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let summary = waf
        .create_ip_set()
        .name("rgn-ipset")
        .scope(Scope::Regional)
        .ip_address_version(aws_sdk_wafv2::types::IpAddressVersion::Ipv4)
        .addresses("10.0.0.0/8")
        .send()
        .await
        .expect("create")
        .summary
        .expect("summary");
    let arn = summary.arn().expect("arn");
    assert!(
        arn.contains(":regional/ipset/rgn-ipset/"),
        "REGIONAL ARN must use the `regional` path segment, got: {arn}"
    );
}

#[tokio::test]
async fn web_acl_without_token_domains_has_no_integration_url() {
    // GetWebACL reports ApplicationIntegrationURL only when the ACL declares
    // token domains; a basic ACL has none, so the field is absent.
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    waf.create_web_acl()
        .name("no-captcha")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("no-captcha"))
        .send()
        .await
        .expect("create");
    let got = waf
        .get_web_acl()
        .name("no-captcha")
        .scope(Scope::Regional)
        .send()
        .await
        .expect("get");
    assert!(
        got.application_integration_url().is_none(),
        "no token domains -> no ApplicationIntegrationURL, got: {:?}",
        got.application_integration_url()
    );
}

#[tokio::test]
async fn web_acl_create_get_update_delete_lifecycle() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;

    let summary = waf
        .create_web_acl()
        .name("test-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("test-acl"))
        .description("E2E test ACL")
        .send()
        .await
        .expect("create")
        .summary
        .expect("summary");
    let id = summary.id().expect("id").to_owned();
    let arn = summary.arn().expect("arn").to_owned();
    let initial_lock = summary.lock_token().expect("lock").to_owned();
    assert!(arn.contains("/webacl/test-acl/"), "unexpected ARN: {arn}");

    let got = waf
        .get_web_acl()
        .name("test-acl")
        .scope(Scope::Regional)
        .id(&id)
        .send()
        .await
        .expect("get");
    let acl = got.web_acl().expect("acl");
    assert_eq!(acl.name(), "test-acl");
    assert_eq!(acl.id(), id);
    let current_lock = got.lock_token().expect("lock").to_owned();
    assert_eq!(current_lock, initial_lock);

    let next_lock = waf
        .update_web_acl()
        .name("test-acl")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&current_lock)
        .default_action(allow_default())
        .visibility_config(vis("test-acl"))
        .description("E2E test ACL updated")
        .send()
        .await
        .expect("update")
        .next_lock_token()
        .expect("next lock")
        .to_owned();
    assert_ne!(next_lock, current_lock, "lock token rotates on update");

    waf.delete_web_acl()
        .name("test-acl")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&next_lock)
        .send()
        .await
        .expect("delete");

    let listed = waf
        .list_web_acls()
        .scope(Scope::Regional)
        .send()
        .await
        .expect("list");
    assert!(listed.web_acls().is_empty());
}

#[tokio::test]
async fn update_web_acl_persists_ddos_and_application_config() {
    use aws_sdk_wafv2::types::{
        ApplicationAttribute, ApplicationConfig, LowReputationMode, OnSourceDDoSProtectionConfig,
    };

    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;

    let summary = waf
        .create_web_acl()
        .name("ddos-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("ddos-acl"))
        .send()
        .await
        .expect("create")
        .summary
        .expect("summary");
    let id = summary.id().expect("id").to_owned();
    let lock = summary.lock_token().expect("lock").to_owned();

    // UpdateWebACL previously dropped OnSourceDDoSProtectionConfig +
    // ApplicationConfig (bug-hunt 2026-06-24, 1.13).
    waf.update_web_acl()
        .name("ddos-acl")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&lock)
        .default_action(allow_default())
        .visibility_config(vis("ddos-acl"))
        .on_source_d_do_s_protection_config(
            OnSourceDDoSProtectionConfig::builder()
                .alb_low_reputation_mode(LowReputationMode::ActiveUnderDdos)
                .build()
                .unwrap(),
        )
        .application_config(
            ApplicationConfig::builder()
                .attributes(
                    ApplicationAttribute::builder()
                        .name("HostHeader")
                        .values("example.com")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("update");

    let got = waf
        .get_web_acl()
        .name("ddos-acl")
        .scope(Scope::Regional)
        .id(&id)
        .send()
        .await
        .expect("get");
    let acl = got.web_acl().expect("acl");
    assert_eq!(
        acl.on_source_d_do_s_protection_config()
            .map(|c| c.alb_low_reputation_mode().clone()),
        Some(LowReputationMode::ActiveUnderDdos)
    );
    let app = acl.application_config().expect("application config");
    assert_eq!(app.attributes().len(), 1);
    assert_eq!(app.attributes()[0].name(), Some("HostHeader"));
}

#[tokio::test]
async fn update_with_stale_lock_token_returns_optimistic_lock_error() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let summary = waf
        .create_web_acl()
        .name("stale")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("stale"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let id = summary.id().expect("id").to_owned();
    let err = waf
        .update_web_acl()
        .name("stale")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token("not-the-real-token")
        .default_action(allow_default())
        .visibility_config(vis("stale"))
        .send()
        .await
        .expect_err("stale token");
    assert!(format!("{err:?}").contains("WAFOptimisticLock"));
}

#[tokio::test]
async fn create_duplicate_web_acl_rejected() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    waf.create_web_acl()
        .name("dupe")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("dupe"))
        .send()
        .await
        .unwrap();
    let err = waf
        .create_web_acl()
        .name("dupe")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("dupe"))
        .send()
        .await
        .expect_err("duplicate");
    assert!(format!("{err:?}").contains("WAFDuplicateItem"));
}

#[tokio::test]
async fn ip_set_lifecycle_with_lock_token_rotation() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let summary = waf
        .create_ip_set()
        .name("blocklist")
        .scope(Scope::Regional)
        .ip_address_version(IpAddressVersion::Ipv4)
        .addresses("198.51.100.0/24")
        .addresses("203.0.113.0/24")
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let id = summary.id().expect("id").to_owned();
    let lock1 = summary.lock_token().expect("lock").to_owned();

    let got = waf
        .get_ip_set()
        .name("blocklist")
        .scope(Scope::Regional)
        .id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(got.ip_set().unwrap().addresses().len(), 2);

    let lock2 = waf
        .update_ip_set()
        .name("blocklist")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&lock1)
        .addresses("198.51.100.0/24")
        .send()
        .await
        .unwrap()
        .next_lock_token
        .unwrap();
    assert_ne!(lock1, lock2);

    let got2 = waf
        .get_ip_set()
        .name("blocklist")
        .scope(Scope::Regional)
        .id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(got2.ip_set().unwrap().addresses().len(), 1);

    waf.delete_ip_set()
        .name("blocklist")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&lock2)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn associate_web_acl_then_disassociate_clears_lookup() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let summary = waf
        .create_web_acl()
        .name("assoc-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("assoc-acl"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let acl_arn = summary.arn().expect("arn").to_owned();
    let alb_arn =
        "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abcdef";

    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn(alb_arn)
        .send()
        .await
        .unwrap();

    let got = waf
        .get_web_acl_for_resource()
        .resource_arn(alb_arn)
        .send()
        .await
        .unwrap();
    assert!(got.web_acl().is_some());

    let listed = waf
        .list_resources_for_web_acl()
        .web_acl_arn(&acl_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.resource_arns().len(), 1);

    waf.disassociate_web_acl()
        .resource_arn(alb_arn)
        .send()
        .await
        .unwrap();

    let after = waf
        .get_web_acl_for_resource()
        .resource_arn(alb_arn)
        .send()
        .await
        .unwrap();
    assert!(after.web_acl().is_none());
}

#[tokio::test]
async fn delete_web_acl_with_associated_resource_rejected() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let summary = waf
        .create_web_acl()
        .name("locked")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("locked"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let acl_arn = summary.arn().expect("arn").to_owned();
    let id = summary.id().expect("id").to_owned();
    let lock = summary.lock_token().expect("lock").to_owned();
    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/x/abc")
        .send()
        .await
        .unwrap();
    let err = waf
        .delete_web_acl()
        .name("locked")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&lock)
        .send()
        .await
        .expect_err("associated resources block delete");
    assert!(format!("{err:?}").contains("WAFAssociatedItem"));
}

#[tokio::test]
async fn check_capacity_counts_statement_leaves() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;

    let ip = waf
        .create_ip_set()
        .name("cap-ip")
        .scope(Scope::Regional)
        .ip_address_version(IpAddressVersion::Ipv4)
        .addresses("198.51.100.0/24")
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let arn = ip.arn().expect("arn").to_owned();

    let stmt = Statement::builder()
        .ip_set_reference_statement(IpSetReferenceStatement::builder().arn(arn).build().unwrap())
        .build();

    let rule = Rule::builder()
        .name("block-bad-ips")
        .priority(1)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis("block-bad-ips"))
        .statement(stmt)
        .build()
        .unwrap();

    let cap = waf
        .check_capacity()
        .scope(Scope::Regional)
        .rules(rule)
        .send()
        .await
        .unwrap()
        .capacity;
    assert!(cap >= 1, "single leaf statement counts as at least 1 WCU");
}

#[tokio::test]
async fn tag_resource_round_trip_on_web_acl() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let arn = waf
        .create_web_acl()
        .name("tagme")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("tagme"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .arn()
        .expect("arn")
        .to_owned();

    waf.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .unwrap();

    let listed = waf
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let info = listed.tag_info_for_resource().expect("info");
    let tags = info.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), "env");
    assert_eq!(tags[0].value(), "prod");

    waf.untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
    let after = waf
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(after
        .tag_info_for_resource()
        .expect("info")
        .tag_list()
        .is_empty());
}

#[tokio::test]
async fn tag_unknown_arn_returns_nonexistent_item_error() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let err = waf
        .list_tags_for_resource()
        .resource_arn("arn:aws:wafv2:us-east-1:123456789012:regional/webacl/missing/00000000")
        .send()
        .await
        .expect_err("missing arn");
    assert!(format!("{err:?}").contains("WAFNonexistentItem"));
}

#[tokio::test]
async fn put_get_delete_logging_configuration_for_web_acl() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let acl_arn = waf
        .create_web_acl()
        .name("loggable")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("loggable"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .arn()
        .expect("arn")
        .to_owned();

    let log_dest = "arn:aws:logs:us-east-1:123456789012:log-group:aws-waf-logs-test:*";
    let cfg = aws_sdk_wafv2::types::LoggingConfiguration::builder()
        .resource_arn(&acl_arn)
        .log_destination_configs(log_dest)
        .build()
        .unwrap();

    waf.put_logging_configuration()
        .logging_configuration(cfg)
        .send()
        .await
        .unwrap();

    let got = waf
        .get_logging_configuration()
        .resource_arn(&acl_arn)
        .send()
        .await
        .unwrap();
    let g = got.logging_configuration().expect("cfg");
    assert_eq!(g.resource_arn(), acl_arn);
    assert_eq!(g.log_destination_configs(), [log_dest.to_string()]);

    waf.delete_logging_configuration()
        .resource_arn(&acl_arn)
        .send()
        .await
        .unwrap();
    let err = waf
        .get_logging_configuration()
        .resource_arn(&acl_arn)
        .send()
        .await
        .expect_err("missing config");
    assert!(format!("{err:?}").contains("WAFNonexistentItem"));
}

#[tokio::test]
async fn rule_group_and_permission_policy_round_trip() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let summary = waf
        .create_rule_group()
        .name("shared-rg")
        .scope(Scope::Regional)
        .capacity(50)
        .visibility_config(vis("shared-rg"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let arn = summary.arn().expect("arn").to_owned();

    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::222222222222:root"}},"Action":"wafv2:GetRuleGroup","Resource":"{arn}"}}]}}"#
    );
    waf.put_permission_policy()
        .resource_arn(&arn)
        .policy(&policy)
        .send()
        .await
        .unwrap();

    let got = waf
        .get_permission_policy()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(got.policy().unwrap_or(""), policy);

    waf.delete_permission_policy()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn create_api_key_then_get_decrypted() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let api_key = waf
        .create_api_key()
        .scope(Scope::Regional)
        .token_domains("api.example.com")
        .token_domains("alt.example.com")
        .send()
        .await
        .unwrap()
        .api_key()
        .expect("api key")
        .to_owned();
    assert!(!api_key.is_empty());

    let decrypted = waf
        .get_decrypted_api_key()
        .scope(Scope::Regional)
        .api_key(&api_key)
        .send()
        .await
        .unwrap();
    let domains = decrypted.token_domains();
    assert_eq!(domains.len(), 2);
    assert!(domains.iter().any(|d| d == "api.example.com"));

    waf.delete_api_key()
        .scope(Scope::Regional)
        .api_key(&api_key)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn list_managed_rule_groups_returns_seeded_set() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let listed = waf
        .list_available_managed_rule_groups()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
    assert!(listed.managed_rule_groups().len() >= 3);
    assert!(listed
        .managed_rule_groups()
        .iter()
        .any(|g| g.name().unwrap_or("") == "AWSManagedRulesCommonRuleSet"));
}

#[tokio::test]
async fn describe_managed_rule_group_returns_real_rules_and_rejects_unknown() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;

    // A known managed group returns its real capacity + rule set, not a
    // fabricated "RuleA" placeholder.
    let desc = waf
        .describe_managed_rule_group()
        .vendor_name("AWS")
        .name("AWSManagedRulesCommonRuleSet")
        .scope(Scope::Regional)
        .send()
        .await
        .expect("describe known group");
    assert_eq!(desc.capacity(), Some(700));
    let rule_names: Vec<&str> = desc.rules().iter().filter_map(|r| r.name()).collect();
    assert!(
        rule_names.contains(&"CrossSiteScripting_BODY"),
        "expected real CRS rules, got {rule_names:?}"
    );
    assert!(!rule_names.contains(&"RuleA"), "must not fabricate RuleA");

    // An unknown group is rejected with WAFInvalidParameterException instead
    // of a fabricated response.
    let err = waf
        .describe_managed_rule_group()
        .vendor_name("AWS")
        .name("AWSManagedRulesNonexistentRuleSet")
        .scope(Scope::Regional)
        .send()
        .await
        .expect_err("unknown group must error");
    let svc = err.into_service_error();
    assert!(
        svc.meta().code() == Some("WAFInvalidParameterException"),
        "unexpected error: {svc:?}"
    );
}

#[tokio::test]
async fn get_mobile_sdk_release_url_uses_provided_platform() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let resp = waf
        .generate_mobile_sdk_release_url()
        .platform(Platform::Ios)
        .release_version("1.0.0")
        .send()
        .await
        .unwrap();
    let url = resp.url().expect("url");
    assert!(
        url.to_ascii_lowercase().contains("ios"),
        "url should contain platform: {url}"
    );
    assert!(url.contains("1.0.0"));
}

/// Phase W1: the WAFv2 evaluator runs entirely in-process. Until the W2
/// dataplane integrations land (ALB / API Gateway / CloudFront), tests
/// drive evaluation through the `/_fakecloud/wafv2/evaluate` admin
/// endpoint. The endpoint accepts a synthetic `WafRequest` plus a WebACL
/// ARN and returns the resolved verdict.
#[tokio::test]
async fn evaluator_blocks_request_from_listed_ip_via_admin_endpoint() {
    use aws_sdk_wafv2::types::{IpSetReferenceStatement, Statement as SdkStatement};

    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let http = reqwest::Client::new();

    // 1) Create an IPSet with the loopback CIDR.
    let ip_set_arn = waf
        .create_ip_set()
        .name("eval-blocked")
        .scope(Scope::Regional)
        .ip_address_version(IpAddressVersion::Ipv4)
        .addresses("203.0.113.0/24")
        .send()
        .await
        .expect("create ipset")
        .summary
        .expect("summary")
        .arn
        .expect("ipset arn");

    // 2) Create a WebACL with a Block-on-IP rule referencing that IPSet.
    let block_rule = Rule::builder()
        .name("block-bad-ips")
        .priority(0)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis("block-bad-ips"))
        .statement(
            SdkStatement::builder()
                .ip_set_reference_statement(
                    IpSetReferenceStatement::builder()
                        .arn(&ip_set_arn)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .expect("rule");
    let acl_arn = waf
        .create_web_acl()
        .name("eval-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("eval-acl"))
        .rules(block_rule)
        .send()
        .await
        .expect("create acl")
        .summary
        .expect("summary")
        .arn
        .expect("acl arn");

    // 3) GetWebACL still returns the rule as configured.
    let scoped_arn_lookup = waf
        .list_web_acls()
        .scope(Scope::Regional)
        .send()
        .await
        .expect("list");
    assert!(scoped_arn_lookup
        .web_acls()
        .iter()
        .any(|s| s.arn() == Some(acl_arn.as_str())));

    // 4) Hit the admin evaluator with a request from a blocked IP.
    let body = serde_json::json!({
        "webAclArn": acl_arn,
        "request": {
            "method": "GET",
            "uri": "/health",
            "sourceIp": "203.0.113.42",
        },
    });
    let resp: serde_json::Value = http
        .post(format!("{}/_fakecloud/wafv2/evaluate", server.endpoint()))
        .json(&body)
        .send()
        .await
        .expect("admin call")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["action"], "Block");
    assert_eq!(resp["blocked"], true);
    assert_eq!(resp["terminatingRuleId"], "block-bad-ips");

    // 5) A request from an unlisted IP falls through to default Allow.
    let body_allow = serde_json::json!({
        "webAclArn": acl_arn,
        "request": {
            "method": "GET",
            "uri": "/health",
            "sourceIp": "10.0.0.5",
        },
    });
    let resp_allow: serde_json::Value = http
        .post(format!("{}/_fakecloud/wafv2/evaluate", server.endpoint()))
        .json(&body_allow)
        .send()
        .await
        .expect("admin call 2")
        .json()
        .await
        .expect("json 2");
    assert_eq!(resp_allow["action"], "Allow");
    assert_eq!(resp_allow["blocked"], false);
}

#[tokio::test]
async fn evaluator_geo_match_uses_country_header_override() {
    use aws_sdk_wafv2::types::{GeoMatchStatement, Statement as SdkStatement};

    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let http = reqwest::Client::new();

    let block_de = Rule::builder()
        .name("block-de")
        .priority(0)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis("block-de"))
        .statement(
            SdkStatement::builder()
                .geo_match_statement(
                    GeoMatchStatement::builder()
                        .country_codes(aws_sdk_wafv2::types::CountryCode::De)
                        .build(),
                )
                .build(),
        )
        .build()
        .expect("rule");
    let acl_arn = waf
        .create_web_acl()
        .name("geo-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("geo-acl"))
        .rules(block_de)
        .send()
        .await
        .expect("create acl")
        .summary
        .expect("summary")
        .arn
        .expect("arn");

    // Header override: x-fakecloud-geo-country=DE -> Block.
    let body = serde_json::json!({
        "webAclArn": acl_arn,
        "request": {
            "uri": "/",
            "headers": [["x-fakecloud-geo-country", "DE"]],
        },
    });
    let resp: serde_json::Value = http
        .post(format!("{}/_fakecloud/wafv2/evaluate", server.endpoint()))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["action"], "Block", "DE country header should block");

    // No header -> default Allow.
    let body2 = serde_json::json!({
        "webAclArn": acl_arn,
        "request": {"uri": "/"},
    });
    let resp2: serde_json::Value = http
        .post(format!("{}/_fakecloud/wafv2/evaluate", server.endpoint()))
        .json(&body2)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp2["action"], "Allow");
}

#[tokio::test]
async fn evaluator_rate_based_blocks_after_limit() {
    use aws_sdk_wafv2::types::{
        RateBasedStatement, RateBasedStatementAggregateKeyType, Statement as SdkStatement,
    };

    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let http = reqwest::Client::new();

    let rate_rule = Rule::builder()
        .name("rate")
        .priority(0)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis("rate"))
        .statement(
            SdkStatement::builder()
                .rate_based_statement(
                    RateBasedStatement::builder()
                        .limit(3)
                        .aggregate_key_type(RateBasedStatementAggregateKeyType::Ip)
                        .evaluation_window_sec(300)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .expect("rule");
    let acl_arn = waf
        .create_web_acl()
        .name("rate-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("rate-acl"))
        .rules(rate_rule)
        .send()
        .await
        .expect("create acl")
        .summary
        .expect("summary")
        .arn
        .expect("arn");

    // Pin the evaluation clock so the test is deterministic.
    let now: i64 = 1_700_000_000;
    let mk_body = || {
        serde_json::json!({
            "webAclArn": acl_arn,
            "request": {
                "uri": "/api",
                "sourceIp": "198.51.100.7",
                "nowEpochSecs": now,
            },
        })
    };
    // First three requests pass.
    for i in 0..3 {
        let resp: serde_json::Value = http
            .post(format!("{}/_fakecloud/wafv2/evaluate", server.endpoint()))
            .json(&mk_body())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp["action"], "Allow", "request {i} should be allowed");
    }
    // The 4th exceeds Limit=3 within the window.
    let blocked: serde_json::Value = http
        .post(format!("{}/_fakecloud/wafv2/evaluate", server.endpoint()))
        .json(&mk_body())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(blocked["action"], "Block");
    assert_eq!(blocked["blocked"], true);
    assert_eq!(blocked["terminatingRuleId"], "rate");
}

#[tokio::test]
async fn paginate_with_stale_marker_does_not_panic() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    waf.create_web_acl()
        .name("only-one")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("only-one"))
        .send()
        .await
        .unwrap();
    let resp = waf
        .list_web_acls()
        .scope(Scope::Regional)
        .next_marker("99999")
        .send()
        .await
        .unwrap();
    assert!(resp.web_acls().is_empty());
    assert!(resp.next_marker().is_none());
}
